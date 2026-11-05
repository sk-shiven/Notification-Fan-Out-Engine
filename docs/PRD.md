# Notification Fan-Out Engine: PRD (v0.1)

**Status:** Draft · **Scope:** Portfolio demo with a reproducible load-test benchmark

## Executive Summary

### Overview & Mission
The Notification Fan-Out Engine is a resilient, event-driven distributed system designed to solve high-volume notification delivery across multiple heterogeneous channels. It decouples event producers from destination endpoints, ensuring that an event published once by an upstream service is reliably fanned out to thousands of matching subscribers (webhooks, email, and future channels) with strict causal ordering per key, deterministic rate limiting, automated fault tolerance, and comprehensive auditability.

The engine serves as a reference architecture for mission-critical event delivery, built upon the **Transactional Outbox Pattern with Change Data Capture (CDC)**, durable distributed message queues, at-least-once delivery with end-to-end idempotency, and non-blocking retry mechanisms.

---

### v1 MVP Scope (Immediate Deliverable)
The v1 MVP focuses on delivering a robust, demonstrable, and locally benchmarkable engine deployed via Docker Compose, capable of sustaining hundreds of events per second on developer hardware:

1. **Ingest & Outbox Layer**:
   - High-throughput REST API (`POST /events`) receiving events with a type, payload, ordering key, and optional client idempotency key.
   - Atomic database write inserting event data and an `outbox` record within a single PostgreSQL ACID transaction, guaranteeing zero event loss upon HTTP 200/201 response.
   - Debezium CDC (Kafka Connect tailing PostgreSQL WAL via logical replication) streaming outbox events directly into Kafka (`events.raw`).

2. **Fan-Out & Routing**:
   - Stream processing worker consuming `events.raw`, resolving active subscriptions by event type, and generating individual delivery items in PostgreSQL.
   - Deduplication guard via database unique constraint on `(event_id, subscription_id)` ensuring idempotent re-processing.
   - Dispatch to channel-specific Kafka topics (`deliveries.webhook`, `deliveries.email`) while preserving message partition ordering keys.

3. **Channel Adapters**:
   - **Webhook Delivery Worker**: HTTP POST dispatcher enforcing per-destination timeouts, generating cryptographic HMAC-SHA256 signatures (`X-Signature-SHA256`), and injecting idempotency headers (`X-Event-ID`, `X-Delivery-ID`, `X-Timestamp`).
   - **Email Delivery Worker**: Dedicated provider adapter (e.g., SMTP / transactional email API) validating address syntax and classifying transient vs. permanent failures.
   - Response code classification: HTTP 2xx marked `DELIVERED`; 429, 5xx, and network timeouts treated as retryable; client 4xx (except 429) treated as terminal failures.

4. **Reliability & Delivery Guarantees**:
   - **At-Least-Once Delivery**: Consumers commit Kafka offsets strictly after the delivery state and attempt history are durably committed to PostgreSQL.
   - **Per-(Ordering Key, Destination) Ordering**: Strict FIFO dispatch. Baseline implements Option A (in-place partition-blocking retry), designed with an abstraction layer to migrate to Option C (parked-key routing to prevent head-of-line blocking).
   - **Exponential Backoff Retries**: Configurable backoff with full jitter to avoid thundering herds.
   - **Dead Letter Queue (DLQ)**: Deliveries exceeding max attempts transition to `DEAD` status and publish to a DLQ topic for operational triage.

5. **Traffic Shaping & Administration**:
   - Per-destination rate limiting (token bucket / leaky bucket) ensuring burst traffic is smoothed and delayed rather than dropped.
   - Administrative REST endpoints for subscriber/subscription CRUD, delivery status lookup (`GET /deliveries/{id}`), DLQ inspection, and manual replay.

6. **Benchmarking & Observability**:
   - One-command reproducible load test suite (`loadtest/generator.py`, mock webhook receiver, `loadtest/report.py`) outputting sustained events/sec, end-to-end latency percentiles (p50, p95, p99), duplicate rates, and fault injection behavior (receiver 500s, slow responses, worker SIGKILL).
   - Core metrics tracking consumer group lag, delivery duration, attempt outcome distributions, and DLQ depth.

---

### Production-Ready Target Architecture (Full Completion)
When fully evolved into an enterprise-grade production platform, the engine incorporates the following core capabilities:

- **Expanded Channel Ecosystem**: Native adapters for Mobile Push (Apple APNs via HTTP/2, Firebase Cloud Messaging v1), SMS (Twilio, AWS SNS, MessageBird with 10DLC compliance), In-App WebSockets / Server-Sent Events, and team messaging webhooks (Slack, MS Teams).
- **Dynamic Content & Templating Engine**: Multi-lingual (i18n/l10n) templating engine (Jinja2 / Handlebars / MJML) supporting rich HTML, plain text, and localized push payloads with dynamic variable substitution and schema validation.
- **Preference & Policy Engine**: Recipient-level notification center managing channel preferences, topic opt-ins/opt-outs, quiet hours (Do-Not-Disturb windows), and frequency capping (e.g., max 3 emails per day).
- **Advanced Traffic Management**: Distributed multi-tenant rate limiting and priority queues (differentiating critical transactional OTPs from bulk marketing notifications) with adaptive backpressure based on downstream provider health.
- **Enterprise Reliability & Disaster Recovery**: Multi-region active-active deployment, automatic multi-provider failover (e.g., SendGrid failing over to AWS SES), and automated dead-letter queue reprocessing workflows.
- **Deep Observability & Compliance**: End-to-end distributed tracing via OpenTelemetry (W3C trace context propagation across API, Kafka, and workers), SOC2 / GDPR compliant PII masking and audit logs, and customizable customer-facing delivery webhooks.

---

### Key Architectural Boundaries & v1 Non-Goals
To ensure deliverability and empirical focus during v1, the following features are explicitly deferred:
- *No SMS or Mobile Push channels* (v1 strictly limits scope to Webhooks and Email).
- *No Visual Template UI or dynamic rendering engine* (payloads are forwarded as received).
- *No multi-tenant organization billing, quota metering, or self-service recipient portals*.
- *No multi-region active-active or Kubernetes cloud orchestration* (targeted runtime is Docker Compose).
- *No exactly-once delivery guarantees across remote networks* (acknowledged as physically impossible; engine guarantees at-least-once with receiver-side deduplication).

---

## 1. Summary

A multi-channel notification delivery engine. Producers publish an event once; the engine fans it out to every matching subscriber (webhook endpoints and email addresses) reliably, in per-key order, with retries, rate limiting, and full delivery observability.

The project exists to demonstrate: the transactional outbox pattern, CDC-based publishing, at-least-once delivery with idempotency, DLQs, and honest measurement of throughput and latency.

## 2. Decisions locked in

| Area | Decision |
|---|---|
| Language | Python (API and workers) |
| Channels (v1) | Webhooks + email |
| Fan-out model | One event → many subscribers/endpoints |
| Outbox relay | CDC with Debezium (Kafka Connect + Postgres logical replication) |
| Must-have reliability | Exponential-backoff retries, per-destination rate limiting, HMAC webhook signing, per-key ordering |
| Runtime | Docker Compose, real email provider |
| Benchmark target | Modest: hundreds of events/sec on a laptop |

## 3. Goals and non-goals

**Goals**
- No accepted event is lost (outbox guarantees the event is durably recorded with the producer's write).
- At-least-once delivery, made effectively-once for well-behaved receivers via idempotency keys.
- Failing destinations never block healthy ones.
- Every delivery attempt is queryable (status, attempts, last error).
- A one-command load test that outputs throughput, latency percentiles, and duplicate rate.

**Non-goals (v1)**
- Exactly-once delivery to external endpoints (not achievable; we document this).
- SMS/push, templates UI, multi-region, cloud deployment.
- Full multi-tenant billing/quotas.

## 4. Architecture

```
Producer API ──(1 txn)──> Postgres: events + outbox
                              │  logical replication
                          Debezium (Kafka Connect)
                              ▼
                     Kafka: events.raw  (key = ordering key)
                              ▼
                  Fan-out worker: match subscriptions,
                  write one delivery row per (event, subscription)
                              ▼
                  Kafka: deliveries.webhook / deliveries.email
                              ▼
        Delivery workers: rate limit → send → record attempt
                    │ success            │ retryable failure     │ exhausted
                 mark delivered     retry path (see §6)      DLQ topic + DB status
```

**Components**
1. **Ingest API**: accepts events, writes `events` and `outbox` rows in one transaction.
2. **Debezium**: streams outbox rows to Kafka.
3. **Fan-out worker**: consumes raw events, resolves subscribers, creates delivery records, emits delivery messages.
4. **Delivery workers** (webhook, email): perform the send, sign payloads, enforce rate limits, record attempts.
5. **Admin/query API**: subscription CRUD, delivery status, DLQ inspection and replay.

## 5. Data model (Postgres, indicative)

- `subscribers` (id, name)
- `subscriptions` (id, subscriber_id, channel, destination, event_types, secret, rate_limit, active)
- `events` (id, type, ordering_key, payload, created_at)
- `outbox` (id, aggregate/event_id, topic, key, payload, created_at)
- `deliveries` (id, event_id, subscription_id, status, attempt_count, next_attempt_at, last_error)
- `delivery_attempts` (id, delivery_id, started_at, duration_ms, status_code/response, error)

Unique constraint on `(event_id, subscription_id)` on `deliveries` is the core idempotency guard on our side.

## 6. Ordering vs. failed deliveries (recommendation)

Ordering key = producer-supplied key (e.g. user ID); it is the Kafka message key, so all events for a key land in one partition. Ordering is guaranteed **per (ordering key, destination)**.

| Option | How it works | Pros | Cons |
|---|---|---|---|
| A. Blocking retry | Worker retries in place; partition stalls | Strict order, simplest | One bad endpoint stalls every key on that partition |
| B. Retry topics, relaxed order | Failures move to retry topics; later events proceed | Isolates failures, simple | Violates the ordering must-have |
| **C. Parked key (recommended)** | Short bounded in-place retries; if still failing, mark `(key, destination)` as *parked* and route that pair's later messages to the retry topic in arrival order; other keys keep flowing | Preserves per-key order and avoids head-of-line blocking | Most complex; needs parked-state storage and careful un-parking |

**Recommendation:** C. It is the only option satisfying both the ordering and "failures don't block healthy traffic" goals, and it is a strong design-discussion story. Fallback if time is short: ship A first, behind an interface that lets C replace it.

## 7. Functional requirements

**FR1 Ingest:** `POST /events` with type, ordering key, payload, and an optional client idempotency key (duplicates return the original event).
**FR2 Fan-out:** one delivery per matching active subscription; re-processing the same Kafka message must not create duplicate deliveries.
**FR3 Webhook delivery:** POST with headers for event ID, delivery ID (idempotency key), timestamp, and an HMAC signature over the body using the subscription secret. Timeout and status-code classification: 2xx success; 429/5xx/timeouts retryable; other 4xx terminal.
**FR4 Email delivery:** send via a provider (open question Q1); classify transient vs permanent failures.
**FR5 Retries:** exponential backoff with jitter, configurable max attempts, then DLQ.
**FR6 Rate limiting:** per-destination limits (Q2 covers where state lives); a limited delivery is delayed, not failed.
**FR7 DLQ:** dead deliveries stored with error history; API to inspect and replay.
**FR8 Observability:** delivery status query; metrics for consumer lag, delivery latency, attempt outcomes, DLQ depth.

## 8. Delivery semantics (stated plainly)

- Events: durable once the ingest transaction commits.
- Deliveries: **at-least-once**. Receivers may see duplicates (worker crash after send, before commit) and should dedupe on the delivery ID header.
- Ordering: per (key, destination), subject to §6.
- Kafka consumers commit offsets only after the delivery state is durably recorded.

## 9. Benchmark plan

Target (proposed, to be validated on your hardware): sustain several hundred events/sec end to end on a laptop with a local mock webhook receiver, with fan-out factor as a parameter.

Report per run: events/sec ingested, deliveries/sec, p50/p95/p99 ingest-to-delivery latency, duplicate rate, DLQ count, and behavior under fault injection (receiver returning 500s, killing a worker mid-run, slow receiver).

Use a mock receiver for throughput runs. Real email providers rate-limit and would distort results, so run email only in a small correctness test.

## 10. Milestones

1. Compose stack: Postgres, Kafka, Kafka Connect/Debezium; outbox events flowing to a topic.
2. Ingest API + fan-out worker + webhook delivery (happy path, HMAC).
3. Retries, DLQ, replay; delivery-attempt records.
4. Per-destination rate limiting; ordering strategy (A, then C).
5. Email channel.
6. Metrics, load-test harness, fault-injection runs, write-up with results.

## 11. Risks

- Debezium/Kafka Connect setup is the largest infrastructure hurdle; verify configuration against current Debezium docs (including the outbox event router).
- Python client choice affects throughput; verify the current state of Kafka client libraries before committing.
- Parked-key logic (§6C) is subtle; needs dedicated tests.
- Laptop benchmarks are noisy; document hardware and settings.

## 12. Open questions

- **Q1:** Which email provider (or SMTP relay) for the real-email path?
- **Q2:** Rate-limit state in Redis, or in Postgres to keep the stack smaller?
- **Q3:** Subscription registration: admin API only, or self-service per subscriber?
- **Q4:** Retention and replay: how long to keep events and delivery attempts?
- **Q5:** Any schema/versioning approach for event payloads (JSON with a version field vs a schema registry)?

---

## 13. v1 MVP Scope & Final Deliverable Summary

### 13.1 Summary of v1 MVP Scope
The primary objective of the v1 MVP is to deliver a functional, resilient, event-driven notification engine that runs locally via Docker Compose, satisfies strict distributed systems guarantees, and can be benchmarked on developer hardware.

#### In-Scope for v1 MVP:
1. **Atomic Ingestion & Transactional Outbox**:
   - High-throughput REST API (`POST /events`) receiving events with `event_type`, `ordering_key`, payload, and an optional `Idempotency-Key` header.
   - Dual-write prevention: writes to `events` and `outbox` within a single PostgreSQL ACID transaction (`BEGIN ... COMMIT`).
   - Exactly-once ingestion semantics for clients sending idempotency keys within a 48-hour deduplication window.

2. **Change Data Capture (CDC) via Debezium**:
   - PostgreSQL logical replication using `pgoutput` and a dedicated replication slot.
   - Kafka Connect running Debezium PostgreSQL connector with Outbox Event Router SMT.
   - Automatic routing of outbox rows to Kafka topic `events.raw`, keyed by `ordering_key`.

3. **Subscription Management & Fan-Out Routing**:
   - In-memory subscription registry (with 60-second TTL cache) matching events by `event_type`.
   - Fan-out worker consuming `events.raw` and creating delivery rows in PostgreSQL.
   - Engine-side idempotency guard via `UNIQUE (event_id, subscription_id)` with `ON CONFLICT DO NOTHING`.
   - Dispatch to channel topics `deliveries.webhook` and `deliveries.email` preserving partition ordering key.

4. **Multi-Channel Delivery Adapters**:
   - **Webhook Channel**: Asynchronous HTTP POST worker with connection pooling, configurable timeouts (5s connect, 10s read), cryptographic HMAC-SHA256 signature generation (`X-Signature-SHA256`), and traceability headers (`X-Event-ID`, `X-Delivery-ID`, `X-Timestamp`).
   - **Email Channel**: Abstract email adapter supporting local SMTP (MailHog/sink) for deterministic testing and AWS SES for cloud deployment.
   - Response classification: HTTP 2xx marked `DELIVERED`; HTTP 429, 5xx, and network timeouts marked retryable; client 4xx (except 429) marked terminal `FAILED`.

5. **Reliability, Retries & Dead Letter Queue (DLQ)**:
   - Exponential backoff with full jitter to eliminate thundering herd problems.
   - At-least-once delivery guarantee: Kafka consumer offsets committed strictly after PostgreSQL attempt persistence.
   - Dead Letter Queue routing: deliveries exceeding `max_attempts` (default 5) or suffering terminal errors transition to status `DEAD` and publish to Kafka topic `deliveries.dlq`.
   - Operational tooling: administrative REST endpoints and CLI tool (`scripts/replay_dlq.py`) for inspecting and replaying dead deliveries.

6. **Traffic Shaping & Causal Ordering**:
   - Distributed rate limiting via Redis Token Bucket (atomic Lua script) enforcing per-subscription requests-per-second ceilings. Throttled deliveries are delayed and rescheduled; they are never dropped.
   - Per-(ordering key, destination) sequential FIFO delivery. Baseline implements Option A (in-place partition-blocking retry with capped backoff); target architecture includes Option C (parked-key routing to prevent head-of-line blocking).

7. **Observability & Empirical Benchmarking**:
   - Prometheus metrics endpoint (`/metrics`) exposing ingestion rate, consumer group lag, delivery duration percentiles, attempt outcomes, and DLQ depth.
   - Delivery query APIs (`GET /deliveries/{id}`, `GET /events/{id}/deliveries`).
   - Standalone benchmark suite (`loadtest/generator.py`, `mock_receiver`, `loadtest/report.py`) measuring sustained QPS (hundreds of events/sec), p50/p95/p99 latency percentiles, duplicate rates, and fault injection behavior (500 errors, slow responses, worker crashes).

#### Out-of-Scope for v1 MVP (Explicit Non-Goals):
- No SMS (Twilio/SNS) or Mobile Push (APNs/FCM) channels.
- No visual template editor, MJML rendering, or dynamic email body interpolation.
- No multi-tenant billing, quotas, or customer self-service web portals.
- No multi-region active-active replication or Kubernetes Helm charts.
- No physically impossible "remote exactly-once" delivery (at-least-once with receiver deduplication is enforced).

---

### 13.2 Core Feature Matrix & Acceptance Criteria

| Feature Area | Component | v1 MVP Capability | Target Acceptance Criteria |
|---|---|---|---|
| **Ingestion** | Ingest API (`POST /events`) | Atomic event + outbox insert | Zero event loss upon HTTP 202; deduplicated on `Idempotency-Key` replay. |
| **CDC Relay** | Debezium + Kafka Connect | WAL logical replication to Kafka | Inserting row in `outbox` results in message on `events.raw` with ordering key. |
| **Fan-Out** | Fan-Out Worker | Event-to-subscription fan-out | 1 event fans out to $N$ channel messages; re-runs produce 0 duplicate delivery rows. |
| **Webhook Delivery** | Webhook Worker | HTTP POST with HMAC-SHA256 | Valid `t={ts},v1={hash}` signature verified by receiver; connect/read timeouts enforced. |
| **Email Delivery** | Email Worker | SMTP / SES Adapter | Valid email sent to sink; bounces (550) classified as terminal failure. |
| **Rate Limiting** | Redis Token Bucket | Per-destination traffic shaping | Deliveries smoothed to $\le R$ req/sec; zero deliveries dropped or failed due to limits. |
| **Retries & DLQ** | Resiliency Engine | Exponential backoff + DLQ | Retries up to 5 times with jitter; transitions to `DEAD` and emits to `deliveries.dlq`. |
| **Ordering** | Ordering Manager | Per-(key, destination) FIFO | Baseline Option A blocks partition; Option C parks key to prevent HoL blocking. |
| **DLQ Replay** | Admin API & CLI | Triage & re-drive dead messages | `POST /dlq/replay` re-enqueues dead delivery to channel queue for retry. |
| **Observability** | Telemetry & Query API | Prometheus `/metrics` & delivery query | Latency histogram (p50/p95/p99), lag gauge, and attempt history returned. |
| **Benchmarking** | Loadtest Suite | Calibrated generator & mock receiver | One-command run sustains hundreds of events/sec, outputs verified latency report. |

---

### 13.3 Final Completion Architecture: Future Evolution
Upon completion of the v1 MVP, the platform's architectural foundation enables straightforward evolution into an enterprise notification infrastructure:
- **Mobile Push & SMS Gateway**: Plug-in adapters for Apple APNs HTTP/2, Firebase Cloud Messaging v1, and Twilio SMS.
- **Dynamic Localization & Templating Engine**: Jinja2/MJML engine with schema validation and localized message bundles.
- **Recipient Preference Center**: Granular opt-in/opt-out preferences, frequency capping (digest rules), and quiet hours.
- **Distributed Multi-Region Active-Active**: Cross-region Kafka MirrorMaker 2, Global Aurora/CockroachDB, and Anycast routing.
- **Automated DLQ Auto-Heal**: Automated backoff probing and self-healing redelivery workflows for recovered endpoints.
