# Notification Fan-Out Engine: Master Technical Architecture & 10-Day Implementation Plan

**Document Version:** 1.0.0  
**Status:** Approved Reference Implementation Plan  
**Target Milestone:** v1 MVP Production-Ready Architecture  
**Scope:** Distributed, fault-tolerant notification delivery engine deployed via Docker Compose with empirical benchmark harness.

---

## 1. System Architecture & Subsystem Decomposition

The Notification Fan-Out Engine is an event-driven, distributed system built on the **Transactional Outbox Pattern with Change Data Capture (CDC)**. It decouples high-throughput event ingestion from asynchronous multi-channel fan-out, enforcing strict causal ordering per key, deterministic rate limiting, automated fault classification, exponential backoff retries, and comprehensive auditability.

```
Producer API ──(1 ACID txn)──> PostgreSQL: [events + outbox]
                                     │  WAL Logical Replication
                               Debezium (Kafka Connect)
                                     ▼
                          Kafka: events.raw (keyed by ordering_key)
                                     ▼
                          Fan-Out Worker:
                          - Resolves active subscriptions
                          - Inserts deliveries (idempotent ON CONFLICT)
                          - Dispatches to channel queues
                                     │
                 ┌───────────────────┴───────────────────┐
                 ▼                                       ▼
       Kafka: deliveries.webhook               Kafka: deliveries.email
                 ▼                                       ▼
       Webhook Delivery Worker                 Email Delivery Worker
       - Redis Token-Bucket Limiter            - Email Provider Adapter
       - HMAC-SHA256 Signing                   - Syntax & Bounce Classification
       - HTTP POST Dispatch                    - SMTP / SES Dispatch
       - Attempt History Persistence           - Attempt History Persistence
                 │                                       │
        ┌────────┴────────┬─────────────┐                │
        ▼                 ▼             ▼                ▼
     Success:         Retryable:    Exhausted:     Retryable / Dead:
   Mark DELIVERED   Backoff Retry   DLQ Topic +    Same Retry/DLQ
   Commit Offset    (Option A / C)  Status DEAD    Lifecycle
```

### Component Directory Mapping

The implementation strictly aligns with the modular codebase structure under `src/fanout/`, `infra/`, and `loadtest/`:

```
Notification-Engine/
├── docs/                                  # Specifications, PRD, status, benchmarks
├── infra/
│   ├── debezium/                          # Kafka Connect & Debezium Outbox SMT JSON
│   ├── kafka/                             # Topic provisioning scripts
│   ├── postgres/                          # SQL migrations, replication role DDL
│   └── redis/                             # Rate limiter scripts
├── loadtest/
│   ├── mock_receiver/                     # Configurable latency & error mock HTTP sink
│   ├── scenarios/                         # Load profile definitions (baseline, fault injection)
│   ├── generator.py                       # High-throughput async load generator
│   └── report.py                          # Percentile latency (p50/p95/p99) & audit reporter
├── scripts/
│   ├── replay_dlq.py                      # CLI tool to inspect and replay dead deliveries
│   └── seed_subscriptions.py              # Test subscriber and subscription provisioning
├── src/fanout/
│   ├── api/                               # FastAPI ingestion, subscription CRUD, delivery query
│   ├── channels/                          # Channel dispatch adapters (Webhook, Email base)
│   ├── common/                            # Config, DB pool, Kafka clients, metrics, models, signing
│   ├── delivery/                          # Delivery worker loops, ordering engines, DLQ handler
│   └── fanout_worker/                     # CDC event consumer & delivery fan-out dispatcher
└── tests/                                 # Unit, integration, and ordering test suites
```

---

### Architectural Boundaries for the 9 Core Subsystems

#### 1. Ingest & Outbox Persistence Subsystem (`src/fanout/api/`)
- **Architectural Boundary:** Stateless HTTP application layer exposing REST endpoints.
- **Responsibilities:**
  - Authenticates incoming producer requests and validates payloads against strict Pydantic schemas.
  - Implements producer-side idempotency via the `Idempotency-Key` header, caching responses to guarantee exactly-once ingestion semantics for well-behaved clients.
  - Commits the event payload into `events` and an outbox record into `outbox` within a **single atomic PostgreSQL ACID transaction** (`BEGIN ... COMMIT`).
  - Guarantees zero event loss: once the producer receives HTTP `202 Accepted` (or `200 OK` on idempotency replay), the event is durably committed to disk.

#### 2. Debezium CDC & Outbox Relay Subsystem (`infra/debezium/`, `infra/postgres/`)
- **Architectural Boundary:** PostgreSQL write-ahead log (WAL) logical replication stream coupled to Kafka Connect.
- **Responsibilities:**
  - Streams row inserts from `outbox` via PostgreSQL `pgoutput` plugin using an isolated replication slot.
  - Applies the Debezium Outbox Event Router Single Message Transform (SMT) (`io.debezium.transforms.outbox.EventRouter`) to unpack aggregate payloads, strip PostgreSQL internal metadata, and route to Kafka topic `events.raw`.
  - Sets the Kafka message key to the event's `ordering_key`, guaranteeing partition affinity.
  - Decouples producer transaction latency from Kafka broker availability.

#### 3. Fan-Out & Subscription Resolution Worker (`src/fanout/fanout_worker/`)
- **Architectural Boundary:** High-throughput streaming consumer process between `events.raw` and channel queues.
- **Responsibilities:**
  - Consumes messages from `events.raw` as part of the `fanout-workers` consumer group.
  - Evaluates active subscriptions matching the incoming `event_type` using an in-memory subscription registry (with TTL cache and invalidation hooks).
  - Performs an idempotent batch insert into the PostgreSQL `deliveries` table guarded by `UNIQUE (event_id, subscription_id)` (`ON CONFLICT DO NOTHING`).
  - Publishes individual delivery tasks to channel-specific Kafka topics (`deliveries.webhook`, `deliveries.email`), preserving the original `ordering_key`.
  - Commits `events.raw` Kafka consumer offsets strictly **after** database delivery rows and channel Kafka messages are acknowledged.

#### 4. Webhook Delivery Worker Subsystem (`src/fanout/delivery/webhook_worker.py`, `src/fanout/channels/webhook.py`)
- **Architectural Boundary:** Outbound HTTP client worker cluster consuming `deliveries.webhook`.
- **Responsibilities:**
  - Consumes delivery tasks, parses destination endpoints, and checks destination rate limits.
  - Computes cryptographic HMAC-SHA256 signatures over timestamp and body using the subscription secret (`X-Signature-SHA256`).
  - Injects traceability headers: `X-Event-ID`, `X-Delivery-ID`, `X-Timestamp`, and `User-Agent`.
  - Dispatches outbound HTTP POST via asynchronous HTTP connection pools (`httpx.AsyncClient`) with strict connect (5s) and read (10s) timeouts.
  - Classifies HTTP response codes: HTTP 2xx as `DELIVERED`; HTTP 429, 5xx, and network timeouts as retryable; client 4xx (except 429) as terminal `FAILED`.
  - Durably records every attempt in `delivery_attempts` and commits Kafka consumer offsets only after state persistence.

#### 5. Email Delivery Worker Subsystem (`src/fanout/delivery/email_worker.py`, `src/fanout/channels/email.py`)
- **Architectural Boundary:** Asynchronous messaging consumer cluster consuming `deliveries.email`.
- **Responsibilities:**
  - Consumes delivery messages and verifies destination email syntax.
  - Dispatches outbound emails through an abstract `EmailChannelAdapter` interface supporting `SMTPEmailAdapter` (local MailHog/sink testing) and `SESEmailAdapter` (AWS Simple Email Service).
  - Classifies delivery responses: SMTP 250 / SES MessageId as success; SMTP 421/450/451 (greylisting, mailbox busy) as retryable; SMTP 550/551/554 (unknown user, mailbox disabled) as terminal failure.
  - Records delivery attempts in `delivery_attempts` and updates delivery status.

#### 6. Distributed Rate Limiter Subsystem (`src/fanout/common/ratelimit.py`)
- **Architectural Boundary:** Centralized traffic-shaping engine backed by Redis.
- **Responsibilities:**
  - Enforces per-destination throughput ceilings (requests per second) defined at the subscription level.
  - Implements an atomic **Token Bucket Algorithm** executed via Redis Lua scripts:
    * Key: `ratelimit:{destination_hash}`
    * Evaluates available tokens and calculates refill delta based on elapsed time.
    * Returns `(allowed: bool, retry_after_ms: int)`.
  - Guarantees non-destructive traffic smoothing: rate-limited deliveries are deferred and rescheduled; they are **never dropped or failed**.

#### 7. Resiliency, Retry Engine, DLQ & Ordering Subsystem (`src/fanout/delivery/ordering.py`, `dlq.py`)
- **Architectural Boundary:** Delivery reliability coordinator managing retry scheduling, dead-letter routing, and causal ordering.
- **Responsibilities:**
  - Computes retry intervals using exponential backoff with full jitter to eliminate thundering herds:
    $$\text{delay} = \text{random.uniform}\left(0, \min(T_{\max}, T_{\text{base}} \times 2^{\text{attempt}})\right)$$
  - Enforces per-key sequential delivery:
    * **Option A (Baseline):** Worker pauses the consumer thread and retries in-place for retryable failures, preserving partition FIFO sequence.
    * **Option C (Target):** Fast bounded in-place retries; if still failing, parks the `(ordering_key, destination)` pair in a Redis registry, routes subsequent messages for that key to `deliveries.parked`, and lets healthy keys flow unblocked.
  - Dead Letter Queue (DLQ): Deliveries exceeding `max_attempts` (default 5) or suffering terminal failures transition to status `DEAD` in PostgreSQL and emit to `deliveries.dlq`.
  - Provides DLQ replay APIs and CLI tooling (`scripts/replay_dlq.py`) for safe operator re-driving.

#### 8. Observability, Telemetry & Admin API Subsystem (`src/fanout/common/metrics.py`, `src/fanout/api/`)
- **Architectural Boundary:** Monitoring and control plane.
- **Responsibilities:**
  - Exposes Prometheus metrics via `/metrics` (ingestion rate, consumer group lag, end-to-end delivery latency histograms, outcome counters, DLQ depth, parked key gauges).
  - Exposes delivery status endpoints: `GET /deliveries/{id}` and `GET /events/{id}/deliveries`.
  - Exposes subscription management CRUD endpoints (`POST`, `GET`, `PUT`, `DELETE /subscriptions`).
  - Provides DLQ triage and replay endpoints (`GET /dlq`, `POST /dlq/replay`).

#### 9. Synthetic Benchmark & Fault Injection Harness (`loadtest/`)
- **Architectural Boundary:** Standalone, out-of-process load generator and mock verification sink.
- **Responsibilities:**
  - `mock_receiver`: High-concurrency FastAPI HTTP receiver with programmable response latency (e.g., 20ms - 2000ms) and dynamic fault injection (e.g., 10% 500s, 5% 429s).
  - `generator.py`: Generates calibrated event loads (200 to 1,000+ events/sec) with parameterized fan-out factors (1:1, 1:5, 1:20) and controlled key distributions.
  - `report.py`: Ingests run metrics and receiver logs to compute verified empirical results: sustained events/sec, deliveries/sec, p50/p95/p99 end-to-end latencies, duplicate rates, and zero out-of-order deliveries.

---

## 2. End-to-End Data Flows & Message Lifecycles

### Flow 1: Ingestion & Outbox Persistence
```
Upstream Client               Ingest API (FastAPI)              PostgreSQL (ACID)
      │                                │                                │
      ├── POST /events ───────────────>│                                │
      │   Headers:                     ├── Check Idempotency-Key        │
      │   - Idempotency-Key: <UUID>    │   in cache / events table ────>│
      │   Payload:                     │<── Key unseen ─────────────────┤
      │   { type, key, payload }       │                                │
      │                                ├── BEGIN TRANSACTION ───────────┤
      │                                │   INSERT INTO events ...       │
      │                                │   INSERT INTO outbox ...       │
      │                                │   COMMIT ──────────────────────┤
      │                                │<── Tx committed (LSN recorded)─┤
      │<── 202 Accepted ───────────────┤                                │
      │    { event_id, status }        │                                │
```

### Flow 2: CDC Capture to Kafka `events.raw`
```
PostgreSQL WAL               Debezium Connector              Kafka: events.raw
      │                               │                              │
      ├── WAL change event ──────────>│                              │
      │   (outbox table INSERT)       │                              │
      │                               ├── Outbox SMT Transformation: │
      │                               │   - Extract JSON payload     │
      │                               │   - Set Key = ordering_key   │
      │                               │   - Set Topic = events.raw   │
      │                               │                              │
      │                               ├── Produce Kafka Record ─────>│
      │                               │<── Broker ACK (acks=all) ────┤
      │<── Acknowledge LSN Position ──┤                              │
```

### Flow 3: Fan-Out Subscription Matching & Channel Dispatch
```
Kafka: events.raw         Fan-Out Worker        PostgreSQL (deliveries)    Kafka: deliveries.webhook
       │                         │                          │                         │
       ├── Poll batch ──────────>│                          │                         │
       │                         ├── Match Subscriptions    │                         │
       │                         │   (in-memory registry)   │                         │
       │                         │                          │                         │
       │                         ├── BEGIN TRANSACTION ─────┤                         │
       │                         │   INSERT INTO deliveries │                         │
       │                         │   ON CONFLICT DO NOTHING │                         │
       │                         │   COMMIT ────────────────┤                         │
       │                         │                          │                         │
       │                         ├── Produce delivery msg ───────────────────────────>│
       │                         │   Key = ordering_key                               │
       │                         │<── Broker ACK ─────────────────────────────────────┤
       │                         │                                                    │
       │<── Commit Kafka offset ─┤                                                    │
```

### Flow 4: Webhook Delivery Dispatch & Outcome Handling
```
Kafka: deliveries.webhook   Webhook Worker     Redis RateLimiter   Target Webhook Server   PostgreSQL
           │                       │                   │                     │                  │
           ├── Poll message ──────>│                   │                     │                  │
           │                       ├── Check Limit ───>│                     │                  │
           │                       │<── Allowed ───────┤                     │                  │
           │                       │                                         │                  │
           │                       ├── Compute HMAC-SHA256                   │                  │
           │                       │                                         │                  │
           │                       ├── HTTP POST (5s/10s timeout) ──────────>│                  │
           │                       │   Headers: X-Signature, X-Delivery-ID   │                  │
           │                       │<── HTTP 200 OK ─────────────────────────┤                  │
           │                       │                                                            │
           │                       ├── BEGIN TRANSACTION ──────────────────────────────────────>┤
           │                       │   UPDATE deliveries SET status='DELIVERED'                 │
           │                       │   INSERT INTO delivery_attempts ...                        │
           │                       │   COMMIT ──────────────────────────────────────────────────┤
           │<── Commit offset ─────┤                                                            │
```

### Flow 5: Retry, DLQ & Parked-Key Progression (Option A vs Option C)
```
Webhook Worker                   Target Host                  PostgreSQL / Redis          Kafka: deliveries.dlq
      │                                │                                │                           │
      ├── HTTP POST ──────────────────>│                                │                           │
      │<── HTTP 503 / Timeout ─────────┤                                │                           │
      │                                                                 │                           │
      ├─ [Option A: In-Place Retry] ────────────────────────────────────┤                           │
      │  Sleep(exponential_backoff)                                     │                           │
      │  Retry HTTP POST up to 5 times                                  │                           │
      │  (Partition paused; strict ordering preserved)                  │                           │
      │                                                                 │                           │
      ├─ [Option C: Parked-Key Transition] ─────────────────────────────┤                           │
      │  If attempts >= 2:                                              │                           │
      │  Mark (key, dest) PARKED in Redis ─────────────────────────────>│                           │
      │  Subsequent msgs for key routed to deliveries.parked ───────────┤                           │
      │  Healthy keys on partition continue without delay!              │                           │
      │                                                                 │                           │
      └─ [Exhaustion / Terminal Failure] ───────────────────────────────┤                           │
         If attempts >= max_attempts:                                   │                           │
         Update deliveries SET status='DEAD' ──────────────────────────>│                           │
         Produce event to Dead Letter Queue ───────────────────────────────────────────────────────>│
         Commit consumer offset to unblock queue ───────────────────────┤                           │
```

---

## 3. Comprehensive Failure Mode Analysis & Mitigation Strategies

| Failure Mode | Root Cause | Immediate System Impact | Architectural Mitigation & Recovery Strategy |
|---|---|---|---|
| **1. Downstream 5xx / Socket Timeout** | Target webhook receiver crashes, hangs, or drops connections | Worker thread blocked on HTTP socket; dispatch fails | Worker enforces strict HTTP client timeouts (5s connect, 10s read). Failures classified as *retryable*. Scheduled for retry using exponential backoff with full jitter. Attempt logged in `delivery_attempts`. |
| **2. Downstream 429 Too Many Requests** | Target receiver throttles incoming traffic | Requests rejected with HTTP 429 | Webhook worker inspects `Retry-After` header. Destination is paused in the local rate limiter for the indicated duration. Delivery is deferred without incrementing terminal failure count. |
| **3. Head-of-Line (HoL) Blocking** | Continuously failing destination stalls partition under Option A | All keys assigned to that Kafka partition are delayed | Under **Option A**: Bounded short retry budget (max 5 attempts, capped at 30s) prevents indefinite stalls. Under **Option C**: After 2 failures, `(key, dest)` is parked in Redis. Subsequent deliveries for that key queue in `deliveries.parked`, allowing healthy keys to proceed immediately. |
| **4. Poison Pill Payload** | Malformed JSON, illegal characters, or unparseable schema | Consumer crashes repeatedly during deserialization | Ingestion API rejects invalid schemas with HTTP 400. If an unparseable message reaches a worker, the deserializer catches `ValidationError`, logs a critical error, routes the raw payload directly to `deliveries.dlq` with reason `CORRUPT_PAYLOAD`, and commits the offset. |
| **5. Worker Crash Mid-Delivery** | OOM killer, SIGKILL, or hardware failure during HTTP send | External call may have executed, but DB state and Kafka offset uncommitted | **At-Least-Once Semantics**: Upon replacement worker restart, Kafka redelivers the uncommitted message. Receiver uses `X-Delivery-ID` header to detect duplicates in its idempotency cache and return HTTP 200 without re-executing side effects. |
| **6. Debezium / Kafka Connect Crash** | Connect worker fails, network partition to Kafka | Outbox records accumulate in PostgreSQL; events.raw paused | PostgreSQL logical replication slot retains WAL on disk safely. Ingestion API continues accepting events (zero ingest downtime). Once Debezium restarts, it streams accumulated WAL changes sequentially from the last confirmed LSN without data loss. |
| **7. Kafka Broker Downtime** | Kafka broker unavailable or restarting | Workers cannot poll or publish messages | Ingestion API writes solely to PostgreSQL (`events` + `outbox`), remaining fully operational. Worker loops catch Kafka connection errors, enter backoff loops, and auto-reconnect once the broker recovers. |
| **8. Redis Rate Limiter Failure** | Redis instance unreachable | Rate limit checks throw connection errors | Configurable fail-open policy: when Redis is unreachable, worker logs a warning and falls back to a local in-memory sliding-window limiter (bounded capacity) to maintain delivery liveness without dropping traffic. |
| **9. PostgreSQL Database Saturation** | Connection pool exhausted during load burst | DB queries time out or throw pool exhaustion errors | Services utilize `asyncpg` with bounded connection pools (e.g., 20 connections per worker). Ingest API utilizes short statement timeouts (2s). Backpressure propagated to HTTP clients via HTTP 503 with `Retry-After`. |

---

## 4. Delivery Guarantees & Consistency Proofs

### 4.1 At-Least-Once Delivery Proof
The engine provides mathematically verifiable **at-least-once delivery** from producer to receiver across all 4 system boundaries:
1. **Producer $\to$ Ingestion API:** Durability is guaranteed by the PostgreSQL ACID transaction committing `events` and `outbox` records before HTTP 202 is returned. If the write fails, the producer receives HTTP 5xx and retries using its `Idempotency-Key`.
2. **Outbox $\to$ Kafka (`events.raw`):** PostgreSQL WAL guarantees that no outbox record is lost. Debezium uses replication slot LSN tracking and Kafka producer `acks=all`. The LSN advances only after Kafka confirms message replication.
3. **Kafka $\to$ Fan-Out Worker:** The consumer processes messages in batches. It commits offsets to Kafka **only after** all corresponding `deliveries` rows are committed to PostgreSQL and channel messages are published to Kafka.
4. **Channel Queue $\to$ Receiver:** The delivery worker commits Kafka offsets **strictly after** the HTTP response attempt is durably recorded in `delivery_attempts` and the delivery status is updated. If the worker crashes mid-send, the uncommitted offset causes Kafka to redeliver the message upon restart.

### 4.2 Multi-Layer Deduplication Windows
To absorb duplicate deliveries produced by network retries or worker restarts, the architecture enforces three distinct deduplication layers:
- **Layer 1: Producer Ingestion Idempotency**
  - Upstream producers supply an `Idempotency-Key` header.
  - Ingestion API verifies against `idx_events_idempotency_key`. Repeated requests within 48 hours return the cached event ID with `200 OK` and bypass outbox re-insertion.
- **Layer 2: Engine Fan-Out Deduplication**
  - PostgreSQL `deliveries` table enforces a composite unique constraint: `UNIQUE (event_id, subscription_id)`.
  - Re-processing an event from `events.raw` executes `ON CONFLICT (event_id, subscription_id) DO NOTHING`. No duplicate delivery rows or channel messages are created.
- **Layer 3: Webhook Receiver Idempotency**
  - Every outbound webhook POST injects:
    * `X-Event-ID`: Unique ID of the underlying event.
    * `X-Delivery-ID`: Deterministic UUID of the delivery row.
  - Receivers store `X-Delivery-ID` in a local cache (e.g., Redis or SQL table with 24h TTL) to detect redelivered requests and return HTTP 200 without repeating downstream side effects.

### 4.3 Sequential Ordering Guarantees
- **Contract:** Strict FIFO delivery order is guaranteed **per `(ordering_key, destination)`**.
- **Mechanics:**
  - Events sharing an `ordering_key` are assigned to the identical Kafka partition across `events.raw`, `deliveries.webhook`, and `deliveries.email`.
  - Kafka guarantees per-partition FIFO ordering.
  - **Option A (Baseline):** A single consumer thread per partition processes messages sequentially. Any retryable failure triggers in-place backoff; subsequent deliveries for all keys on that partition wait until the failure resolves or transitions to DLQ.
  - **Option C (Parked-Key Target):** If a `(key, dest)` pair fails repeatedly, it is marked *parked* in Redis. New deliveries for that key are diverted to a parked queue, while deliveries for unrelated keys on the partition continue processing without interruption. When the destination recovers, buffered messages are drained in strict timestamp sequence.

---

## 5. Component Interface Contracts & Schemas

### 5.1 Database Schema DDL (`infra/postgres/init.sql`)

```sql
-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Custom Types
CREATE TYPE channel_type AS ENUM ('webhook', 'email');
CREATE TYPE delivery_status AS ENUM ('PENDING', 'PROCESSING', 'DELIVERED', 'RETRYING', 'PARKED', 'FAILED', 'DEAD');

-- 1. Subscribers
CREATE TABLE subscribers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Subscriptions
CREATE TABLE subscriptions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscriber_id UUID NOT NULL REFERENCES subscribers(id) ON DELETE CASCADE,
    channel channel_type NOT NULL,
    destination VARCHAR(1024) NOT NULL,
    event_types TEXT[] NOT NULL,
    secret VARCHAR(255) NOT NULL,
    rate_limit_per_sec INT NOT NULL DEFAULT 50,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_subscriptions_active_events ON subscriptions USING GIN (event_types) WHERE active = TRUE;
CREATE INDEX idx_subscriptions_subscriber ON subscriptions(subscriber_id);

-- 3. Events
CREATE TABLE events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key VARCHAR(255) UNIQUE,
    event_type VARCHAR(128) NOT NULL,
    ordering_key VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_events_ordering_key ON events(ordering_key);
CREATE INDEX idx_events_created_at ON events(created_at);

-- 4. Transactional Outbox (Debezium CDC Target)
CREATE TABLE outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregatetype VARCHAR(255) NOT NULL,
    aggregateid VARCHAR(255) NOT NULL,
    type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 5. Deliveries
CREATE TABLE deliveries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id UUID NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    subscription_id UUID NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
    channel channel_type NOT NULL,
    destination VARCHAR(1024) NOT NULL,
    status delivery_status NOT NULL DEFAULT 'PENDING',
    attempt_count INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 5,
    next_attempt_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_event_subscription UNIQUE (event_id, subscription_id)
);

CREATE INDEX idx_deliveries_status_next ON deliveries(status, next_attempt_at);
CREATE INDEX idx_deliveries_event_id ON deliveries(event_id);
CREATE INDEX idx_deliveries_destination ON deliveries(destination);

-- 6. Delivery Attempts (Audit & Telemetry)
CREATE TABLE delivery_attempts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    delivery_id UUID NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
    attempt_number INT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    duration_ms INT NOT NULL,
    status_code INT,
    response_body TEXT,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_delivery_attempts_delivery_id ON delivery_attempts(delivery_id);
```

### 5.2 Kafka Topic Specifications & Envelopes

| Topic Name | Partitions | Key | Value Schema Description |
|---|:---:|---|---|
| `events.raw` | 12 | `ordering_key` | Debezium outbox event envelope with raw event payload. |
| `deliveries.webhook` | 12 | `ordering_key` | Webhook delivery task containing delivery ID, secret, destination, and body. |
| `deliveries.email` | 6 | `ordering_key` | Email delivery task containing delivery ID, recipient address, and body. |
| `deliveries.dlq` | 3 | `delivery_id` | Dead-lettered delivery record with error history and failure reason. |

#### Webhook Delivery Message Envelope (`deliveries.webhook`):
```json
{
  "delivery_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "event_id": "1e8c75d2-f435-4299-8cf0-df6042db621a",
  "subscription_id": "f5d08f32-7231-4c19-9118-8d4e9c7198bb",
  "ordering_key": "tenant_cust_88291",
  "channel": "webhook",
  "destination": "https://api.partner.com/webhooks/orders",
  "secret": "whsec_live_948a271bca492e",
  "rate_limit_per_sec": 50,
  "payload": {
    "order_id": "ord_9921",
    "amount": 149.50,
    "status": "COMPLETED"
  },
  "created_at": "2026-11-05T10:00:00.000Z",
  "attempt_count": 0
}
```

### 5.3 REST API Interface Models (FastAPI / Pydantic)

#### Ingest Request: `POST /events`
```http
POST /events HTTP/1.1
Host: api.notification-engine.local
Content-Type: application/json
Idempotency-Key: 7b31c9a8-e4b2-4d51-9fa6-0c1a8519e912

{
  "event_type": "order.completed",
  "ordering_key": "user_49102",
  "payload": {
    "order_id": "ord_9921",
    "total": 149.50,
    "currency": "USD"
  }
}
```

#### Ingest Response: `202 Accepted`
```json
{
  "status": "accepted",
  "event_id": "1e8c75d2-f435-4299-8cf0-df6042db621a",
  "ordering_key": "user_49102",
  "created_at": "2026-11-05T10:00:00.045Z"
}
```

#### Delivery Query Response: `GET /deliveries/{delivery_id}`
```json
{
  "delivery_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "event_id": "1e8c75d2-f435-4299-8cf0-df6042db621a",
  "channel": "webhook",
  "destination": "https://api.partner.com/webhooks/orders",
  "status": "DELIVERED",
  "attempt_count": 2,
  "max_attempts": 5,
  "attempts": [
    {
      "attempt_number": 1,
      "started_at": "2026-11-05T10:00:01.100Z",
      "duration_ms": 115,
      "status_code": 503,
      "error_message": "Service Unavailable"
    },
    {
      "attempt_number": 2,
      "started_at": "2026-11-05T10:00:02.450Z",
      "duration_ms": 82,
      "status_code": 200,
      "error_message": null
    }
  ]
}
```

### 5.4 Webhook Security & HMAC-SHA256 Header Contract

When dispatching to subscriber webhooks, the worker sends:
```http
POST /webhooks/orders HTTP/1.1
Host: api.partner.com
User-Agent: NotificationEngine/1.0
Content-Type: application/json
X-Event-ID: 1e8c75d2-f435-4299-8cf0-df6042db621a
X-Delivery-ID: 9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d
X-Timestamp: 1730799000
X-Signature-SHA256: t=1730799000,v1=5d41402abc4b2a76b9719d911017c5921829e01a8b948a9271c6d9a8e2b8c910

{
  "order_id": "ord_9921",
  "total": 149.50,
  "currency": "USD"
}
```

**Signature Calculation Specification:**
- The signature payload is constructed as: `f"{timestamp}.{raw_body_utf8}"`.
- The signature is calculated using HMAC with SHA256 and the subscription secret:
  $$\text{signature} = \text{HMAC-SHA256}(\text{key}=\text{secret}, \text{msg}=f"{t}.{body}")$$
- The header value is formatted as: `t={timestamp},v1={hex_digest}`.

---

## 6. Deep Component Build Specifications & Engineering Blueprints

This section provides the exhaustive, file-by-file engineering blueprints for each of the 9 core subsystems identified in the PRD v1 MVP scope. Every component includes exact class and module hierarchies, data models, algorithm pseudocode, failure handling rules, and verification gates.

---

### Component 1: Ingestion API & Outbox Persistence Engine

#### 1. Module Layout & Target Files
- `src/fanout/api/main.py`: FastAPI application entry point, lifecycle events (db pool init/teardown), CORS, and route mounting.
- `src/fanout/api/routes_events.py`: Endpoint definition for `POST /events`.
- `src/fanout/api/routes_subscriptions.py`: CRUD endpoints for subscribers and subscriptions (`/subscriptions`).
- `src/fanout/api/outbox.py`: `OutboxWriter` domain service managing atomic database persistence.
- `src/fanout/common/models.py`: SQLAlchemy declarative models (`Event`, `Outbox`, `Subscriber`, `Subscription`) and Pydantic DTOs.
- `src/fanout/common/db.py`: `asyncpg` engine and async session factory.

#### 2. Data Models & Schemas
```python
# Pydantic Schemas (src/fanout/common/models.py)
class EventCreateRequest(BaseModel):
    event_type: str = Field(..., max_length=128, example="order.completed")
    ordering_key: str = Field(..., max_length=255, example="user_88219")
    payload: Dict[str, Any] = Field(..., example={"order_id": "ord_9921", "amount": 149.50})

class EventResponse(BaseModel):
    status: str = Field("accepted", example="accepted")
    event_id: UUID
    ordering_key: str
    created_at: datetime
```

#### 3. Step-by-Step Implementation Blueprint
1. **Dependency Injection**: Inject `AsyncSession` into route handler via `Depends(get_db_session)`.
2. **Idempotency Gate**:
   - Inspect request header `Idempotency-Key: Optional[str]`.
   - If present: perform fast lookup in PostgreSQL:
     ```sql
     SELECT id, event_type, ordering_key, created_at FROM events WHERE idempotency_key = :key;
     ```
   - If existing record found: immediately return HTTP `200 OK` with existing event details (idempotency hit; skip outbox insertion).
3. **Atomic Dual-Write (`outbox.py`)**:
   - Open single ACID transaction block:
     ```python
     async with session.begin():
         new_event = Event(
             id=uuid4(),
             idempotency_key=idempotency_key,
             event_type=request.event_type,
             ordering_key=request.ordering_key,
             payload=request.payload,
             created_at=datetime.utcnow()
         )
         session.add(new_event)
         
         # Debezium Outbox Event Router envelope
         outbox_record = Outbox(
             id=uuid4(),
             aggregatetype="event",
             aggregateid=str(new_event.id),
             type=new_event.event_type,
             payload={
                 "event_id": str(new_event.id),
                 "event_type": new_event.event_type,
                 "ordering_key": new_event.ordering_key,
                 "payload": new_event.payload,
                 "created_at": new_event.created_at.isoformat()
             },
             created_at=datetime.utcnow()
         )
         session.add(outbox_record)
     # Transaction commits here; WAL LSN is durably recorded
     ```
4. **Return HTTP 202 Accepted**: Return `EventResponse` with `event_id` and timestamp.

#### 4. Failure Modes & Edge Cases
- **Database Unavailable**: If connection fails, catch `asyncpg.PostgresError`, log error, and return HTTP `503 Service Unavailable` with `Retry-After: 5`.
- **Payload Schema Violation**: Pydantic validation automatically returns HTTP `422 Unprocessable Entity`.
- **Duplicate Key Race Condition**: If two identical idempotency keys arrive concurrently, database unique constraint `uq_events_idempotency_key` triggers an `IntegrityError`; catch and retry query to return the winning record.

#### 5. Verification Gate
```bash
# Ingest event with idempotency key
curl -i -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: test-uuid-001" \
  -d '{"event_type": "user.signup", "ordering_key": "user_42", "payload": {"email": "alice@example.com"}}'
# Expect HTTP 202 Accepted

# Repeat exact request
curl -i -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: test-uuid-001" \
  -d '{"event_type": "user.signup", "ordering_key": "user_42", "payload": {"email": "alice@example.com"}}'
# Expect HTTP 200 OK with identical event_id; 0 duplicate outbox rows in DB
```

---

### Component 2: Debezium CDC & PostgreSQL Logical Replication Relay

#### 1. Module Layout & Target Files
- `infra/postgres/init.sql`: Custom PostgreSQL configuration, database types, tables, replication user, and publication.
- `infra/debezium/outbox-connector.json`: JSON configuration for Debezium Kafka Connect Outbox SMT.
- `infra/debezium/register-connector.sh`: Health-check and automated registration script.
- `docker-compose.yml`: Services for `postgres`, `kafka`, `kafka-connect`, and `redis`.

#### 2. Infrastructure Configuration Blueprint
`infra/postgres/init.sql` grants replication privileges and defines publication:
```sql
-- Create replication-enabled user
CREATE USER debezium_user WITH REPLICATION PASSWORD 'debezium_pass';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium_user;

-- Create publication strictly for outbox table
CREATE PUBLICATION outbox_pub FOR TABLE outbox;
```

`infra/debezium/outbox-connector.json`:
```json
{
  "name": "notification-outbox-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "plugin.name": "pgoutput",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "debezium_user",
    "database.password": "debezium_pass",
    "database.dbname": "notification_db",
    "database.server.name": "pg-outbox",
    "table.include.list": "public.outbox",
    "publication.name": "outbox_pub",
    "slot.name": "debezium_outbox_slot",
    "tombstones.on.delete": "false",
    "transforms": "outbox",
    "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
    "transforms.outbox.table.fields.additional.placement": "type:header:eventType",
    "transforms.outbox.route.by.field": "aggregatetype",
    "transforms.outbox.route.topic.replacement": "events.raw",
    "transforms.outbox.table.expand.json.payload": "true",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}
```

#### 3. Step-by-Step Implementation Blueprint
1. Configure PostgreSQL container command in `docker-compose.yml`:
   ```yaml
   command: ["postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=4", "-c", "max_wal_senders=4"]
   ```
2. In `register-connector.sh`:
   - Poll `http://localhost:8083/connectors` until Kafka Connect returns HTTP 200.
   - Execute `curl -s -X POST -H "Content-Type: application/json" --data @infra/debezium/outbox-connector.json http://localhost:8083/connectors`.
   - Poll connector status at `http://localhost:8083/connectors/notification-outbox-connector/status` verifying state is `RUNNING`.

#### 4. Failure Modes & Edge Cases
- **Replication Slot Orphaned**: If container restarts abruptly, verify slot state via `SELECT * FROM pg_replication_slots;`.
- **Connector Deserialization Failure**: `expand.json.payload=true` requires valid JSON in `outbox.payload`; API guarantees JSON validity prior to insert.

#### 5. Verification Gate
```bash
# Insert direct test row in PostgreSQL
psql -h localhost -U postgres -d notification_db -c "
INSERT INTO outbox (id, aggregatetype, aggregateid, type, payload) 
VALUES (gen_random_uuid(), 'event', 'order_123', 'order.completed', '{\"order_id\": \"123\"}');"

# Consume from Kafka events.raw
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic events.raw --from-beginning --max-messages 1 --property print.key=true
# Verify record is emitted with key 'order_123'
```

---

### Component 3: Fan-Out & Subscription Resolution Worker

#### 1. Module Layout & Target Files
- `src/fanout/fanout_worker/worker.py`: Main asynchronous consumer loop.
- `src/fanout/fanout_worker/registry.py`: Subscription resolution registry with TTL caching.
- `src/fanout/common/kafka.py`: `AIOKafkaConsumer` and `AIOKafkaProducer` wrappers.

#### 2. Data Models & Schemas
```python
# Subscription In-Memory DTO
@dataclass(frozen=True)
class ActiveSubscription:
    id: UUID
    channel: str # 'webhook' | 'email'
    destination: str
    secret: str
    rate_limit_per_sec: int
```

#### 3. Step-by-Step Implementation Blueprint
1. **Initialize Consumer**:
   - Topic: `events.raw`, Consumer Group: `fanout-workers`, `enable_auto_commit=False`.
2. **Subscription Registry Cache**:
   - Cache subscriptions in memory with a 60-second TTL:
     ```python
     async def get_subscriptions_for_event(self, event_type: str) -> List[ActiveSubscription]:
         if self._is_expired():
             await self._refresh_cache()
         return [s for s in self._cache if event_type in s.event_types and s.active]
     ```
3. **Batch Processing Loop**:
   ```python
   async for msg_batch in consumer.getmany(max_records=500, timeout_ms=1000).values():
       for msg in msg_batch:
           event = json.loads(msg.value)
           event_id = UUID(event["event_id"])
           ordering_key = msg.key.decode("utf-8") if msg.key else event.get("ordering_key", "")
           
           # 1. Resolve matching active subscriptions
           matching = await registry.get_subscriptions_for_event(event["event_type"])
           if not matching:
               continue
           
           # 2. Batch insert delivery rows (Idempotency Guard)
           deliveries_to_create = [
               {"id": uuid4(), "event_id": event_id, "subscription_id": sub.id, 
                "channel": sub.channel, "destination": sub.destination, "status": "PENDING"}
               for sub in matching
           ]
           
           async with db_session.begin():
               await db_session.execute(
                   insert(Delivery).values(deliveries_to_create)
                   .on_conflict_do_nothing(index_elements=["event_id", "subscription_id"])
               )
           
           # 3. Dispatch to channel topics
           for deliv, sub in zip(deliveries_to_create, matching):
               topic = f"deliveries.{sub.channel}"
               delivery_envelope = {
                   "delivery_id": str(deliv["id"]),
                   "event_id": str(event_id),
                   "subscription_id": str(sub.id),
                   "ordering_key": ordering_key,
                   "channel": sub.channel,
                   "destination": sub.destination,
                   "secret": sub.secret,
                   "rate_limit_per_sec": sub.rate_limit_per_sec,
                   "payload": event["payload"],
                   "created_at": event["created_at"],
                   "attempt_count": 0
               }
               await producer.send(topic, key=ordering_key.encode("utf-8"), value=json.dumps(delivery_envelope).encode("utf-8"))
           
       # 4. Commit Kafka offset after all DB and channel produces complete
       await consumer.commit()
   ```

#### 4. Failure Modes & Edge Cases
- **Crash Between DB Insert and Kafka Produce**: Upon worker restart, Kafka redelivers `events.raw`. The database insert executes `ON CONFLICT DO NOTHING`, safely preventing duplicate delivery rows while allowing message dispatch to resume.
- **Zero Matching Subscriptions**: Consumer logs debug message, marks offset, and proceeds without overhead.

#### 5. Verification Gate
```bash
# Seed 3 subscriptions for event_type 'order.completed' (2 webhook, 1 email)
python scripts/seed_subscriptions.py

# Ingest 1 event
curl -X POST http://localhost:8000/events -H "Content-Type: application/json" \
  -d '{"event_type": "order.completed", "ordering_key": "user_100", "payload": {"id": 1}}'

# Verify 3 rows in deliveries table
psql -h localhost -U postgres -d notification_db -c "SELECT id, channel, status FROM deliveries;"
# Verify 2 messages on deliveries.webhook and 1 on deliveries.email
```

---

### Component 4: Webhook Delivery Worker & Cryptographic Signing

#### 1. Module Layout & Target Files
- `src/fanout/delivery/webhook_worker.py`: Async consumer for `deliveries.webhook`.
- `src/fanout/channels/webhook.py`: `WebhookDispatcher` class encapsulating `httpx.AsyncClient`.
- `src/fanout/common/signing.py`: HMAC-SHA256 signature generator.
- `loadtest/mock_receiver/server.py`: Test webhook sink validating signatures and headers.

#### 2. Cryptographic Signing Implementation (`src/fanout/common/signing.py`)
```python
def compute_webhook_signature(secret: str, timestamp: int, payload_json: str) -> str:
    canonical = f"{timestamp}.{payload_json}".encode("utf-8")
    signature = hmac.new(secret.encode("utf-8"), canonical, hashlib.sha256).hexdigest()
    return f"t={timestamp},v1={signature}"
```

#### 3. Step-by-Step Implementation Blueprint
1. **Initialize HTTP Client**:
   ```python
   limits = httpx.Limits(max_keepalive_connections=50, max_connections=200)
   timeout = httpx.Timeout(connect=5.0, read=10.0, write=5.0, pool=5.0)
   client = httpx.AsyncClient(limits=limits, timeout=timeout)
   ```
2. **Worker Processing Loop**:
   - Poll message from `deliveries.webhook`.
   - **Rate Limit Check**: Query `RedisTokenBucketLimiter.check_limit(msg.destination, msg.rate_limit_per_sec)`.
     - If throttled: delay task for `wait_ms` or pause partition.
   - **Construct Request**:
     - Timestamp: `int(time.time())`.
     - Payload string: `json.dumps(msg.payload, separators=(',', ':'))`.
     - Signature: `compute_webhook_signature(msg.secret, timestamp, payload_str)`.
     - Headers:
       ```http
       Content-Type: application/json
       User-Agent: NotificationEngine-Dispatcher/1.0
       X-Event-ID: {msg.event_id}
       X-Delivery-ID: {msg.delivery_id}
       X-Timestamp: {timestamp}
       X-Signature-SHA256: {signature}
       ```
   - **Dispatch HTTP POST**:
     - Measure duration: `start = time.perf_counter()`.
     - Record outcome into `delivery_attempts` table.
   - **Classify Response**:
     * `2xx`: Update `deliveries` status to `DELIVERED`. Commit offset.
     * `429` / `5xx` / Timeout: Classified as *retryable*. Calculate backoff; trigger retry loop (Option A / C).
     * `4xx` (except 429): Classified as terminal *FAILED*. Transition to status `FAILED`, emit to DLQ, commit offset.

#### 4. Failure Modes & Edge Cases
- **Slow Receiver**: Connect/read timeout aborts request after 10s; logged as retryable error.
- **Malformed Secret**: Catch cryptographic exceptions; flag delivery as terminal configuration error.

#### 5. Verification Gate
```bash
# Run mock receiver on port 8088
python loadtest/mock_receiver/server.py --port 8088 &

# Dispatch delivery task; mock receiver logs:
# [200 OK] Verified HMAC signature for Delivery ID 9b1deb4d... in 14ms
# Check delivery status in PostgreSQL: status = 'DELIVERED', attempt_count = 1
```

---

### Component 5: Email Delivery Worker & Adapter Subsystem

#### 1. Module Layout & Target Files
- `src/fanout/channels/email.py`: `EmailChannelAdapter` ABC, `SMTPEmailAdapter`, and `SESEmailAdapter`.
- `src/fanout/delivery/email_worker.py`: Consumer loop on `deliveries.email`.

#### 2. Abstract Adapter & Error Classification Blueprint
```python
class EmailChannelAdapter(ABC):
    @abstractmethod
    async def send(self, recipient: str, subject: str, body: str) -> DeliveryOutcome:
        pass

class SMTPEmailAdapter(EmailChannelAdapter):
    async def send(self, recipient: str, subject: str, body: str) -> DeliveryOutcome:
        # Validate recipient syntax
        if not re.match(r"[^@]+@[^@]+\.[^@]+", recipient):
            return DeliveryOutcome(status="FAILED", is_retryable=False, error="Invalid recipient address")
        try:
            async with aiosmtplib.SMTP(hostname=self.host, port=self.port) as client:
                code, msg = await client.sendmail(self.sender, [recipient], body)
                if code == 250:
                    return DeliveryOutcome(status="DELIVERED", is_retryable=False)
                elif code in (421, 450, 451): # Transient SMTP greylisting
                    return DeliveryOutcome(status="RETRYING", is_retryable=True, error=f"SMTP {code}: {msg}")
                else: # 550, 551, 554 permanent bounce
                    return DeliveryOutcome(status="FAILED", is_retryable=False, error=f"SMTP {code}: {msg}")
        except (aiosmtplib.SMTPException, OSError) as e:
            return DeliveryOutcome(status="RETRYING", is_retryable=True, error=str(e))
```

#### 3. Step-by-Step Implementation Blueprint
1. Worker consumes `deliveries.email`.
2. Validates email destination format; malformed emails instantly fail without outbound network attempts.
3. Invokes configured adapter (`SMTPEmailAdapter` in dev/Compose; `SESEmailAdapter` in production).
4. Persists attempt to `delivery_attempts` and commits Kafka offset.

#### 4. Failure Modes & Edge Cases
- **Mail Server Down**: Catches `OSError` / socket connection refused; schedules exponential backoff.
- **Hard Bounce (550)**: Permanently invalid recipient marked `FAILED` immediately, avoiding redundant retries.

#### 5. Verification Gate
```bash
# Verify email delivery against MailHog / mock SMTP sink
psql -h localhost -U postgres -d notification_db -c "
SELECT d.destination, d.status, a.status_code, a.duration_ms 
FROM deliveries d JOIN delivery_attempts a ON d.id = a.delivery_id 
WHERE d.channel = 'email';"
```

---

### Component 6: Distributed Rate Limiter Subsystem

#### 1. Module Layout & Target Files
- `src/fanout/common/ratelimit.py`: `RedisTokenBucketLimiter` class and in-memory fallback.
- `infra/redis/token_bucket.lua`: Atomic token bucket Lua script.

#### 2. Atomic Lua Token Bucket Algorithm (`infra/redis/token_bucket.lua`)
```lua
local key = KEYS[1]
local now = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])      -- tokens added per second
local capacity = tonumber(ARGV[3])  -- maximum burst bucket size
local requested = tonumber(ARGV[4]) -- tokens requested (typically 1)

local data = redis.call('HMGET', key, 'tokens', 'last_updated')
local tokens = tonumber(data[1])
local last_updated = tonumber(data[2])

if tokens == nil then
    tokens = capacity
    last_updated = now
else
    local delta_sec = math.max(0, now - last_updated) / 1000.0
    tokens = math.min(capacity, tokens + delta_sec * rate)
    last_updated = now
end

if tokens >= requested then
    tokens = tokens - requested
    redis.call('HMSET', key, 'tokens', tokens, 'last_updated', last_updated)
    redis.call('EXPIRE', key, math.ceil(capacity / rate) + 60)
    return {1, 0} -- Allowed: 1, wait_ms: 0
else
    local missing = requested - tokens
    local wait_ms = math.ceil((missing / rate) * 1000)
    return {0, wait_ms} -- Throttled: 0, wait_ms
end
```

#### 3. Step-by-Step Implementation Blueprint
1. Register and cache SHA of Lua script upon application startup:
   ```python
   self.lua_sha = await redis.script_load(LUA_SCRIPT_TEXT)
   ```
2. In delivery worker:
   - Calculate MD5/SHA256 hash of `destination` to construct key: `ratelimit:{dest_hash}`.
   - Execute: `res = await redis.evalsha(self.lua_sha, 1, key, now_ms, rate_per_sec, capacity, 1)`.
   - If `res[0] == 0`: delivery is throttled. Worker pauses execution for `res[1]` ms using `asyncio.sleep` (or partition pause) and retries the rate check.
3. **Fail-Open Fallback**: If Redis raises `ConnectionError`, log warning and fall back to local bounded in-memory sliding window limiter.

#### 4. Verification Gate
```bash
# Configure subscription with rate_limit_per_sec = 5
# Ingest 25 events in under 1 second
# Inspect mock receiver logs: deliveries arrive evenly spaced at 5 requests/sec over 5 seconds; 0 dropped.
```

---

### Component 7: Resiliency, Backoff Retries, Ordering & DLQ Subsystem

#### 1. Module Layout & Target Files
- `src/fanout/common/retry.py`: Exponential backoff with full jitter formula.
- `src/fanout/delivery/ordering.py`: `OrderingCoordinator` (Option A and Option C engines).
- `src/fanout/delivery/dlq.py`: Dead Letter Queue producer and persistence coordinator.
- `scripts/replay_dlq.py`: CLI tool for DLQ inspection and replay.

#### 2. Mathematical Backoff Blueprint (`src/fanout/common/retry.py`)
```python
def calculate_backoff(attempt: int, base_sec: float = 1.0, max_sec: float = 60.0, factor: float = 2.0) -> float:
    ceiling = min(max_sec, base_sec * (factor ** attempt))
    # Full jitter: uniform random between 0 and ceiling
    return random.uniform(0.0, ceiling)
```

#### 3. Ordering State Machine (Option A vs Option C)
- **Option A (Baseline In-Place Retries)**:
  1. On retryable failure, worker pauses consumer partition: `consumer.pause(partition)`.
  2. Compute `delay = calculate_backoff(delivery.attempt_count)`.
  3. Sleep for `delay` seconds.
  4. Resume partition: `consumer.resume(partition)` and retry dispatch.
  5. If `attempt_count >= max_attempts`: transition to DLQ.
- **Option C (Parked-Key Engine - Target Architecture)**:
  1. Key `(ordering_key, destination)` fails $\ge 2$ times.
  2. Worker adds pair to Redis set `parked_keys`.
  3. Diverts message to Kafka topic `deliveries.parked` and commits offset on `deliveries.webhook`.
  4. Subsequent deliveries for the same key arriving on `deliveries.webhook` check `parked_keys`; if present, they are immediately re-routed to `deliveries.parked`.
  5. Healthy keys for other destinations/users on the same partition continue flowing with zero latency!
  6. Background probe task tests failing destination every 10s. Once destination returns HTTP 200, it drains `deliveries.parked` in chronological sequence and removes key from `parked_keys`.

#### 4. Dead Letter Queue & Replay Blueprint (`dlq.py`, `scripts/replay_dlq.py`)
1. **Transition to DEAD**:
   ```python
   async with db_session.begin():
       await db_session.execute(
           update(Delivery).where(Delivery.id == delivery_id)
           .values(status="DEAD", last_error=error_msg, updated_at=datetime.utcnow())
       )
   await dlq_producer.send("deliveries.dlq", key=delivery_id.bytes, value=json.dumps(dlq_envelope).encode("utf-8"))
   ```
2. **Replay Flow**:
   - `python scripts/replay_dlq.py --all-dead`:
     - Queries PostgreSQL: `SELECT * FROM deliveries WHERE status = 'DEAD'`.
     - Resets `attempt_count = 0`, sets `status = 'PENDING'`.
     - Re-produces delivery envelope to `deliveries.webhook` or `deliveries.email`.

#### 5. Verification Gate
```bash
# Set mock receiver to return 500
python loadtest/mock_receiver/server.py --port 8088 --always-fail &

# Ingest event; watch worker log exponential retries (e.g. 1.2s, 3.8s, 7.1s, 14.5s, 31.0s)
# Verify delivery row transitions to status = 'DEAD'
# Verify message published on deliveries.dlq
# Run replay CLI: python scripts/replay_dlq.py --all-dead
# Verify delivery transitions back to PENDING and is re-attempted
```

---

### Component 8: Observability, Metrics & Administration API

#### 1. Module Layout & Target Files
- `src/fanout/common/metrics.py`: Prometheus metric collectors.
- `src/fanout/api/routes_deliveries.py`: Delivery status lookup and query endpoints.
- `src/fanout/api/routes_dlq.py`: DLQ inspection and administrative replay endpoints.

#### 2. Prometheus Metric Registry Catalog (`src/fanout/common/metrics.py`)
```python
EVENTS_INGESTED = Counter("engine_events_ingested_total", "Total accepted events", ["event_type", "status"])
CONSUMER_LAG = Gauge("engine_consumer_lag", "Kafka consumer group lag", ["consumer_group", "topic", "partition"])
DELIVERIES_TOTAL = Counter("engine_deliveries_total", "Delivery attempts by channel and outcome", ["channel", "status"])
DELIVERY_DURATION = Histogram(
    "engine_delivery_duration_seconds",
    "End-to-end delivery duration from event creation to terminal state",
    ["channel"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0)
)
DLQ_DEPTH = Gauge("engine_dlq_depth", "Current number of dead-lettered deliveries in database", ["channel"])
PARKED_KEYS = Gauge("engine_parked_keys_active", "Number of currently parked (ordering_key, destination) pairs", ["channel"])
```

#### 3. Administrative REST Endpoints
- `GET /metrics`: Standard Prometheus scrape target.
- `GET /deliveries/{delivery_id}`: Returns status, channel, attempt history, and timing:
  ```json
  {
    "delivery_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
    "status": "DELIVERED",
    "attempts": [
      {"attempt": 1, "duration_ms": 110, "status_code": 503, "error": "Service Unavailable"},
      {"attempt": 2, "duration_ms": 45, "status_code": 200, "error": null}
    ]
  }
  ```
- `GET /events/{event_id}/deliveries`: Queries all delivery rows associated with an event.
- `POST /dlq/replay`: Accepts `{"delivery_ids": ["..."]}`, resetting deliveries for re-execution.

#### 4. Verification Gate
```bash
curl -s http://localhost:8000/metrics | grep engine_deliveries_total
# Returns: engine_deliveries_total{channel="webhook",status="DELIVERED"} 42
```

---

### Component 9: Synthetic Benchmark & Fault Injection Harness

#### 1. Module Layout & Target Files
- `loadtest/mock_receiver/server.py`: High-concurrency mock webhook receiver with programmable fault injection.
- `loadtest/generator.py`: Asynchronous event load generator.
- `loadtest/report.py`: Latency percentile (p50/p95/p99) and audit reporter.
- `loadtest/scenarios/baseline.json`: Baseline load configuration.
- `loadtest/scenarios/fault_injection.json`: Unreliable receiver & worker crash profile.

#### 2. Mock Receiver Implementation Blueprint (`loadtest/mock_receiver/server.py`)
```python
# FastAPI app simulating network conditions
@app.post("/webhook")
async def webhook_sink(request: Request):
    # 1. Simulate network latency
    delay = random.uniform(MIN_DELAY_MS, MAX_DELAY_MS) / 1000.0
    await asyncio.sleep(delay)
    
    # 2. Simulate random 5xx failure
    if random.random() < ERROR_RATE:
        return Response(content="Internal Server Error", status_code=500)
    
    # 3. Simulate rate limit 429
    if random.random() < RATE_LIMIT_RATE:
        return Response(content="Too Many Requests", status_code=429, headers={"Retry-After": "1"})
    
    # 4. Verify HMAC signature
    signature = request.headers.get("X-Signature-SHA256")
    body = await request.body()
    # Log delivery receipt for audit and duplicate detection
    received_log.append((request.headers.get("X-Delivery-ID"), time.time()))
    return Response(content="OK", status_code=200)
```

#### 3. Benchmark Load Generator (`loadtest/generator.py`)
- Emits calibrated HTTP POST requests to `/events` sustaining 200–1,000+ QPS.
- Distributes keys across a Zipfian distribution to simulate hot keys alongside long-tail keys.
- Configures fan-out ratios: 1:1, 1:5, and 1:20 subscribers per event.

#### 4. Statistical Analysis & Reporting Blueprint (`loadtest/report.py`)
- Computes:
  - **Sustained Throughput:** Total Ingested Events / Duration, Deliveries / Duration.
  - **Latency Percentiles:** $p_{50}, p_{90}, p_{95}, p_{99}, \max$ of (delivery timestamp $-$ event ingestion timestamp).
  - **Duplicate Rate:**
    $$\text{Duplicate Rate} = \frac{\text{Total Webhook Deliveries Received} - \text{Unique Delivery IDs}}{\text{Unique Delivery IDs}} \times 100\%$$
  - **Ordering Integrity:** Verifies that for every `(ordering_key, destination)`, delivery timestamps strictly satisfy $T_1 \le T_2 \le \dots \le T_n$.

#### 5. Verification Gate
```bash
# Execute end-to-end benchmark run
make loadtest
# Outputs formatted report:
# ========================================================
# BENCHMARK RESULTS: 500 QPS (Duration: 60s, Fan-Out: 1:5)
# Ingestion QPS: 502.4 events/sec
# Delivery QPS:  2,510.1 deliveries/sec
# Latency: p50 = 34ms | p95 = 88ms | p99 = 142ms
# Duplicate Rate: 0.02% (absorbed by idempotency cache)
# Out-of-order Deliveries: 0 (100% strictly ordered)
# DLQ Count: 0
# ========================================================
```

---

## 7. Deepened Day-by-Day Implementation Roadmap (Days 1–10)

### Day 1: Infrastructure, Logical Replication & Database Foundations
- **Target Files:** `docker-compose.yml`, `pyproject.toml`, `Makefile`, `.env.example`, `infra/postgres/init.sql`, `infra/kafka/topics.sh`, `infra/debezium/outbox-connector.json`, `src/fanout/common/config.py`, `src/fanout/common/db.py`.
- **Architectural Responsibilities:**
  1. Build Docker Compose infrastructure running PostgreSQL 16 (`wal_level=logical`, `max_replication_slots=4`, `max_wal_senders=4`), Kafka in KRaft mode, Kafka Connect with Debezium 2.5, and Redis 7.2.
  2. Implement full PostgreSQL schema in `infra/postgres/init.sql` with tables, indexes, constraints, and replication role grants.
  3. Author `infra/kafka/topics.sh` to initialize Kafka topics (`events.raw`, `deliveries.webhook`, `deliveries.email`, `deliveries.dlq`).
  4. Author Debezium Outbox Event Router connector configuration in `infra/debezium/outbox-connector.json`.
  5. Setup Python package management via `pyproject.toml` with `fastapi`, `uvicorn`, `sqlalchemy[asyncio]`, `asyncpg`, `aiokafka`, `httpx`, `redis`, `pydantic-settings`, `prometheus-client`, `pytest`.
- **Failure Modes Handled:** PostgreSQL connection drops, WAL replication slot creation errors, Kafka broker connection timeouts.
- **Verification Gate:** `docker compose up -d` succeeds; running `topics.sh` creates all 4 topics; registering Debezium connector via HTTP POST succeeds; manually inserting a test row into `outbox` results in a message appearing on Kafka topic `events.raw`.

### Day 2: Ingestion API & Outbox Persistence Subsystem
- **Target Files:** `src/fanout/api/main.py`, `src/fanout/api/routes_events.py`, `src/fanout/api/routes_subscriptions.py`, `src/fanout/api/outbox.py`, `src/fanout/common/models.py`.
- **Architectural Responsibilities:**
  1. Define Pydantic request and response schemas for events and subscriptions.
  2. Implement `src/fanout/api/outbox.py`: database helper executing atomic insert into `events` and `outbox` within a single `async with session.begin()` block.
  3. Implement client idempotency: parse `Idempotency-Key` header; if key exists in `events`, return original response with HTTP 200 without creating outbox record.
  4. Implement Subscription CRUD endpoints (`POST`, `GET`, `PUT`, `DELETE /subscriptions`).
- **Failure Modes Handled:** Database connection failure (returns HTTP 503), malformed event payloads (returns HTTP 400), conflicting idempotency keys (returns HTTP 409).
- **Verification Gate:** Single `POST /events` writes to both tables and triggers CDC emission to `events.raw`; repeating identical `POST /events` with the same `Idempotency-Key` returns the original event ID with 0 duplicate outbox rows.

### Day 3: Fan-Out Worker & Channel Routing
- **Target Files:** `src/fanout/fanout_worker/worker.py`, `src/fanout/common/kafka.py`.
- **Architectural Responsibilities:**
  1. Build `FanoutWorker` consuming `events.raw` using `AIOKafkaConsumer` in group `fanout-workers`.
  2. Query active subscriptions matching `event.event_type` from a cached subscription registry (with 60s TTL).
  3. Batch insert delivery records into `deliveries` table using `ON CONFLICT (event_id, subscription_id) DO NOTHING`.
  4. Publish delivery messages to corresponding channel topics (`deliveries.webhook`, `deliveries.email`), preserving the original `ordering_key` as partition key.
  5. Commit `events.raw` offset **only after** DB rows and Kafka channel messages are confirmed.
- **Failure Modes Handled:** Worker crashes before offset commit (idempotent DB replay), Kafka channel broker unavailable (retry loop), zero matching subscriptions (clean no-op commit).
- **Verification Gate:** Ingest 1 event matching 3 webhook subscriptions; verify exactly 3 delivery rows appear in PostgreSQL and exactly 3 messages appear on `deliveries.webhook`. Re-consuming the same Kafka message produces 0 duplicate delivery rows.

### Day 4: Webhook Delivery Engine & Signature Verification
- **Target Files:** `src/fanout/delivery/webhook_worker.py`, `src/fanout/channels/webhook.py`, `src/fanout/common/signing.py`, `loadtest/mock_receiver/server.py`.
- **Architectural Responsibilities:**
  1. Implement `signing.py`: HMAC-SHA256 signature generator (`t={timestamp},v1={hash}`).
  2. Implement `WebhookChannelAdapter`: `httpx.AsyncClient` dispatcher with connection pooling, 5s connect / 10s read timeouts, and tracking headers (`X-Event-ID`, `X-Delivery-ID`, `X-Timestamp`, `X-Signature-SHA256`).
  3. Implement `WebhookWorker` loop consuming `deliveries.webhook`.
  4. Implement response classification: 2xx success; 429/5xx/timeout retryable; 4xx non-retryable terminal.
  5. Durably insert attempt history into `delivery_attempts` and commit Kafka offset.
  6. Implement standalone `mock_receiver` server to verify signatures and header contracts.
- **Failure Modes Handled:** Hanging socket connections (timeout abort), remote server 500s, signature calculation errors.
- **Verification Gate:** Webhook worker delivers an event to `mock_receiver`; mock receiver verifies HMAC signature matches; delivery status marked `DELIVERED` in PostgreSQL; offset committed.

### Day 5: Resiliency, Backoff Retries & Dead-Letter Queue (Option A)
- **Target Files:** `src/fanout/common/retry.py`, `src/fanout/delivery/dlq.py`, `src/fanout/delivery/ordering.py`, `src/fanout/api/routes_deliveries.py`, `scripts/replay_dlq.py`.
- **Architectural Responsibilities:**
  1. Implement exponential backoff with full jitter in `retry.py`.
  2. Implement Option A In-Place Retry Loop: on retryable failure, worker sleeps for backoff duration, preserving partition FIFO sequence.
  3. Implement DLQ routing: if `attempt_count >= max_attempts`, update status to `DEAD`, produce message to Kafka topic `deliveries.dlq`, and commit consumer offset.
  4. Implement DLQ inspection and replay endpoint (`POST /dlq/replay`) and CLI tool (`scripts/replay_dlq.py`).
- **Failure Modes Handled:** Permanent remote endpoint failure, partition stalls, duplicate replay attempts.
- **Verification Gate:** Mock receiver configured to return 500 errors; verify worker retries up to max attempts with exponential backoff, writes attempt records, transitions delivery to `DEAD`, and publishes to `deliveries.dlq`. Triggering replay re-enqueues delivery for retry.

### Day 6: Distributed Rate Limiting Subsystem
- **Target Files:** `src/fanout/common/ratelimit.py`, `infra/redis/`.
- **Architectural Responsibilities:**
  1. Implement Redis Token Bucket rate limiter using an atomic Lua script:
     - Keys: `ratelimit:{destination_hash}`
     - Evaluates rate ceiling per second, current token balance, and refill timestamp.
     - Returns `[allowed (1/0), retry_after_ms]`.
  2. Integrate rate check into `WebhookWorker` dispatch loop.
  3. If throttled, delay message execution without failing or dropping the delivery (deferral handling).
- **Failure Modes Handled:** Destination traffic bursts, Redis connectivity failure (fail-open fallback to bounded local token bucket).
- **Verification Gate:** Configure subscription rate limit to 10 req/sec; burst 50 events; verify mock receiver receives traffic at strictly $\le 10$ req/sec with 0 dropped or failed deliveries.

### Day 7: Email Channel Integration & Error Taxonomy
- **Target Files:** `src/fanout/delivery/email_worker.py`, `src/fanout/channels/email.py`.
- **Architectural Responsibilities:**
  1. Define `EmailChannelAdapter` abstract interface with `SMTPEmailAdapter` (MailHog/local sink) and `SESEmailAdapter`.
  2. Implement email address validation and error classification:
     - 250 / 200: Success -> `DELIVERED`.
     - 421 / 450: Transient greylisting -> retryable.
     - 550 / 554: Unknown user / invalid address -> terminal `FAILED`.
  3. Implement `EmailWorker` consumer on `deliveries.email`.
- **Failure Modes Handled:** Mail server connection drop, malformed email addresses, recipient mailbox full.
- **Verification Gate:** Valid email dispatches to SMTP sink; malformed/bouncing address marked terminal `FAILED` without redundant retries.

### Day 8: Observability, Metrics & Query API
- **Target Files:** `src/fanout/common/metrics.py`, `src/fanout/api/routes_deliveries.py`, `loadtest/report.py`.
- **Architectural Responsibilities:**
  1. Instrument services with Prometheus metrics (`engine_events_ingested_total`, `engine_consumer_lag`, `engine_delivery_attempts_total`, `engine_delivery_latency_seconds`, `engine_dlq_deliveries_total`).
  2. Expose `/metrics` endpoint in API service.
  3. Implement delivery query endpoints: `GET /deliveries/{id}` and `GET /events/{id}/deliveries`.
- **Failure Modes Handled:** High cardinality metric explosion (bounded label values), query timeout under high row counts (optimized indexes).
- **Verification Gate:** Scraping `/metrics` exposes populated histograms and counters; query endpoints return complete historical attempt breakdown for any delivery ID.

### Day 9: Benchmarks, Load Testing & Fault Injection
- **Target Files:** `loadtest/generator.py`, `loadtest/scenarios/`, `loadtest/report.py`.
- **Architectural Responsibilities:**
  1. Build async high-concurrency event generator in `loadtest/generator.py` sustaining 200 to 1,000+ events/sec.
  2. Execute baseline throughput scenario and calculate p50, p95, p99 latency.
  3. Execute fault-injection scenarios:
     - Receiver returning 10% 500 errors and 5% 429s.
     - Worker killed mid-run (`kill -9`) to verify at-least-once redelivery and zero message loss.
  4. Output benchmark summary report via `loadtest/report.py`.
- **Failure Modes Handled:** Client-side event dropouts, benchmark noise, memory leaks during prolonged runs.
- **Verification Gate:** Single command `make loadtest` runs scenario, verifies zero message loss, and prints formatted report with p50/p95/p99 latency percentiles and duplicate rates.

### Day 10: Advanced Ordering (Option C Parked Keys) & System Polish
- **Target Files:** `src/fanout/delivery/ordering.py`, `docs/benchmarks/results.md`, `README.md`.
- **Architectural Responsibilities:**
  1. Implement Option C Parked-Key Engine:
     - Redis registry tracking `(ordering_key, destination_id)` parked state.
     - On repeated failures, key is parked; subsequent deliveries for that key divert to `deliveries.parked`.
     - Background unparking worker periodically probes destination; upon recovery, drains parked queue in order.
  2. Finalize comprehensive test suites in `tests/` (unit, integration, ordering).
  3. Save benchmark performance data and hardware specifications in `docs/benchmarks/`.
  4. Finalize README with quickstart instructions and architecture diagrams.
- **Failure Modes Handled:** Unparking race conditions, out-of-order queue drains, memory leaks in parked registry.
- **Verification Gate:** Benchmark under failing destination demonstrates that unaffected keys on the same partition process with zero delay, and parked keys drain in strict sequential order upon recovery.

---

## 8. Critical Risks, Slip Points & Scope Management

### Identified High-Risk Points
1. **Debezium & Logical Replication Setup (Day 1):**
   - *Risk:* PostgreSQL `wal_level=logical` permissions, replication slot configuration, and Debezium SMT transforms can be finicky.
   - *Mitigation:* Verified JSON connector definition, single-step setup script, and direct verification gate on Day 1 before developing workers.
2. **Head-of-Line Blocking vs. Strict Ordering (Day 5 & Day 10):**
   - *Risk:* Option A causes partition stalls under destination outages. Option C adds stateful parked-key complexity.
   - *Mitigation:* Implement Option A first behind a strict interface; isolate Option C to Day 10 as an advanced stretch milestone.
3. **Local Benchmark Distortion (Day 9):**
   - *Risk:* External network latency (e.g. real email providers) distorts throughput benchmarks.
   - *Mitigation:* Webhook throughput benchmarks use the high-performance local `mock_receiver`; email testing is restricted to a small correctness-only suite.

### Scope Cutting Triage Order
If time constraints require reducing scope during implementation:
1. **First to cut:** Option C Parked-Key ordering (fall back to Option A blocking retries with short max attempt budget).
2. **Second to cut:** Real third-party email provider integration (fall back to local SMTP sink / MailHog).
3. **NEVER cut:** The reproducible benchmark harness (`loadtest/`) and at-least-once delivery with HMAC webhook signatures; these form the core engineering foundation of the project.
