# Notification Fan-Out Engine — Project Status & Implementation Tracking

**Last Updated:** November 7, 2026  
**Current Milestone:** Day 1 — Infrastructure, Logical Replication & Database Foundations  
**Overall Completion:** 5.0% (Source Code & Infrastructure) / 50.0% (Architecture, PRD & Planning)  
**Target Architecture Version:** v1 MVP (Production-Ready Architecture Reference)  

---

## 1. Executive Summary

The **Notification Fan-Out Engine** is a high-reliability, distributed notification delivery system designed to solve high-throughput event dispatch across multiple heterogeneous channels (Webhooks, Email) with strict per-key ordering, rate limiting, and failure isolation. It serves as an empirical implementation of key distributed systems patterns:
- **Transactional Outbox Pattern** with PostgreSQL Write-Ahead Log (WAL) logical replication.
- **Change Data Capture (CDC)** via Kafka Connect and Debezium Outbox Event Router SMT.
- **At-Least-Once Delivery** with end-to-end idempotency guarantees.
- **Causal Per-Key Ordering** (Option A in-place retry baseline; Option C parked-key non-blocking target).
- **Per-Destination Rate Limiting** using distributed token buckets.
- **Dead Letter Queue (DLQ)** with audit trails and administrative replay tooling.
- **Empirical Benchmarking Harness** reporting verified p50/p95/p99 latencies, throughput, and duplicate rates.

### Status Dashboard
| Metric | Status | Detail |
|---|---|---|
| **Project Phase** | **Phase 1: Infrastructure & Foundation** | Python environment initialized; Docker daemon verified; Day 1 foundation underway. |
| **Source Code Progress** | **5.0%** | `pyproject.toml`, `.venv`, editable package `fanout`, `scripts/start_docker.sh`, `.env.example`, `Makefile`, and unit tests established. |
| **Documentation Progress** | **80.0%** | `PRD.md` (Executive Summary + Section 13 v1 MVP Scope & Final Deliverables), `planner.md` (Master Plan + Section 6 Deep Component Blueprints for all 9 subsystems), `arch_v1.png`, and `arch_v2.png`. `architecture.md` and `delivery-semantics.md` are next. |
| **Infrastructure Status** | **Docker Active** | Docker daemon running (v29.4.3). `docker-compose.yml`, SQL init scripts, and Debezium configs are next. |
| **Test Suite** | **Active (1 test)** | Smoke test passing via pytest (`tests/unit/test_env.py`). |
| **Active Environment Blocker** | **None** | Host Docker daemon is running; uv virtualenv installed and verified in editable mode. |

---

## 2. Milestone-by-Milestone Progress Tracking

The table below tracks project progress against the 10-day implementation schedule defined in `docs/planner.md`:

| Milestone | Target Scope | Current Status | % Complete | Key Artifacts | Blockers / Prerequisites |
|---|---|:---:|:---:|---|---|
| **Day 1: Infrastructure & Schema** | Postgres schema, logical replication, Debezium outbox connector, Kafka topics, `docker-compose.yml`, `pyproject.toml`. | In Progress | 35% | `pyproject.toml`, `.venv`, `scripts/start_docker.sh`, `Makefile`, `.env.example` (populated); `docker-compose.yml`, `infra/postgres/init.sql`, `infra/kafka/topics.sh`, `infra/debezium/outbox-connector.json` pending. | Docker daemon active; Python environment configured. |
| **Day 2: Ingest API** | `POST /events` writing `events` + `outbox` in one transaction, client idempotency keys, subscription CRUD endpoints. | Not Started | 0% | `src/fanout/api/main.py`, `routes_events.py`, `routes_subscriptions.py`, `outbox.py` (all 0 B) | Day 1 database schema and connection pool. |
| **Day 3: Fan-Out Worker** | Consume `events.raw`, resolve matching active subscriptions, insert `deliveries` (idempotent), publish to channel topics (`deliveries.webhook`, `deliveries.email`). | Not Started | 0% | `src/fanout/fanout_worker/worker.py`, `src/fanout/common/kafka.py` (all 0 B) | Day 1 Debezium CDC pipeline and Day 2 subscription records. |
| **Day 4: Webhook Delivery** | Webhook sender, HMAC-SHA256 signature, headers (`X-Event-ID`, `X-Delivery-ID`), response classification, attempt records, mock receiver. | Not Started | 0% | `src/fanout/channels/webhook.py`, `src/fanout/delivery/webhook_worker.py`, `src/fanout/common/signing.py` (all 0 B) | Day 3 delivery messages on Kafka. |
| **Day 5: Retries, DLQ & Ordering (Option A)** | Exponential backoff + jitter, DLQ routing, `DEAD` status, blocking retry per partition (Option A), replay script. | Not Started | 0% | `src/fanout/common/retry.py`, `src/fanout/delivery/dlq.py`, `src/fanout/delivery/ordering.py`, `scripts/replay_dlq.py` (all 0 B) | Day 4 webhook delivery and attempt history tables. |
| **Day 6: Rate Limiting** | Per-destination token bucket limiter, delay/deferral handling without message loss. | Not Started | 0% | `src/fanout/common/ratelimit.py` (0 B) | Storage choice decision (Redis token bucket recommended). |
| **Day 7: Email Channel** | Email channel adapter, transient vs permanent bounce classification, live correctness test. | Not Started | 0% | `src/fanout/channels/email.py`, `src/fanout/delivery/email_worker.py` (all 0 B) | Email provider / SMTP relay selection. |
| **Day 8: Observability & Harness** | Prometheus metrics (lag, latency, outcomes, DLQ depth), delivery status API, load generator, reporting CLI. | Not Started | 0% | `src/fanout/common/metrics.py`, `loadtest/generator.py`, `loadtest/report.py` (all 0 B) | Functional delivery pipeline. |
| **Day 9: Benchmarks & Fault Injection** | Baseline benchmark, fault injection (receiver 500s, slow receiver, worker crash), tuning batch sizes and partitions. | Not Started | 0% | `docs/benchmarks/` (empty directory) | Day 8 load generator and mock receiver. |
| **Day 10: Stretch & Polish** | Parked-key ordering (Option C), comprehensive test suites, README architecture walkthrough. | In Progress (Docs only) | 10% | `docs/arch_v1.png`, `docs/arch_v2.png` committed; code and docs pending. | Milestones 1–9 completion. |

---

## 3. Detailed Repository Audit & Artifact State

### 3.1 Git Repository Log & Commit History
- **Active Branch:** `main` (synchronized with `origin/main`)
- **Total Commits:** Exactly 3 commits recorded:
  ```
  commit cd170ce8b18fa10b8f77b74056d018175eb0b2a2 (HEAD -> main, origin/main)
  Author: Shiven Kathuria <shiven.25bcs10290@sst.scaler.com>
  Date:   Wed Sep 30 17:22:00 2026 +0530
      docs: arch_v2 + task_planner
      - Added docs/planner.md (4,199 bytes, 98 lines)
      - Added docs/arch_v2.png (105 KB binary)
      - Renamed docs/arch-visualiser.png -> docs/arch_v1.png

  commit 727f55589721edd2d6ad6777882427e479b2ecf0
  Author: Shiven Kathuria <shiven.25bcs10290@sst.scaler.com>
  Date:   Wed Sep 30 16:13:48 2026 +0530
      init: repo struct
      - Created 35 skeleton files (all 0 bytes) across infra, src, loadtest, scripts, tests

  commit 769d611bdd611f5a675bda2f7a7ce5ec3074328e
  Author: Shiven Kathuria <shiven.25bcs10290@sst.scaler.com>
  Date:   Wed Sep 30 16:06:51 2026 +0530
      init: PRD + arch diagram
      - Added docs/PRD.md (8,155 bytes, 134 lines)
      - Added docs/arch-visualiser.png (1.7 MB binary)
  ```

### 3.2 File-by-File Inventory & Byte Sizing
Across the repository, exactly **39 files** exist:
- **4 populated files:**
  1. `docs/PRD.md` — Complete requirements document with newly integrated Executive Summary.
  2. `docs/planner.md` — Master technical architecture and deepened 10-day build plan (658 lines).
  3. `docs/arch_v1.png` — Architectural visualizer diagram v1 (1.7 MB binary).
  4. `docs/arch_v2.png` — Architectural visualizer diagram v2 (105 KB binary).
- **35 empty placeholder files (0 bytes):**
  - **Build & Configuration:**
    * `pyproject.toml` (0 B)
    * `Makefile` (0 B)
    * `.env.example` (0 B)
    * `docker-compose.yml` (0 B)
    * `README.md` (0 B)
  - **Documentation Placeholders:**
    * `docs/architecture.md` (0 B)
    * `docs/delivery-semantics.md` (0 B)
  - **Infrastructure Configuration:**
    * `infra/postgres/init.sql` (0 B)
    * `infra/debezium/outbox-connector.json` (0 B)
    * `infra/kafka/topics.sh` (0 B)
  - **API Layer:**
    * `src/fanout/api/main.py` (0 B)
    * `src/fanout/api/outbox.py` (0 B)
    * `src/fanout/api/routes_events.py` (0 B)
    * `src/fanout/api/routes_subscriptions.py` (0 B)
    * `src/fanout/api/routes_deliveries.py` (0 B)
  - **Channels:**
    * `src/fanout/channels/webhook.py` (0 B)
    * `src/fanout/channels/email.py` (0 B)
  - **Delivery Workers & Logic:**
    * `src/fanout/fanout_worker/worker.py` (0 B)
    * `src/fanout/delivery/base.py` (0 B)
    * `src/fanout/delivery/webhook_worker.py` (0 B)
    * `src/fanout/delivery/email_worker.py` (0 B)
    * `src/fanout/delivery/ordering.py` (0 B)
    * `src/fanout/delivery/dlq.py` (0 B)
  - **Common Utilities:**
    * `src/fanout/common/config.py` (0 B)
    * `src/fanout/common/db.py` (0 B)
    * `src/fanout/common/kafka.py` (0 B)
    * `src/fanout/common/models.py` (0 B)
    * `src/fanout/common/ratelimit.py` (0 B)
    * `src/fanout/common/retry.py` (0 B)
    * `src/fanout/common/signing.py` (0 B)
    * `src/fanout/common/metrics.py` (0 B)
  - **Scripts & Tools:**
    * `scripts/seed_subscriptions.py` (0 B)
    * `scripts/replay_dlq.py` (0 B)
  - **Load Testing:**
    * `loadtest/generator.py` (0 B)
    * `loadtest/report.py` (0 B)
- **Empty Directories (no files):**
  - `migrations/`
  - `docs/benchmarks/`
  - `loadtest/mock_receiver/`
  - `loadtest/scenarios/`
  - `tests/unit/`
  - `tests/integration/`
  - `tests/ordering/`

---

## 4. Gap Analysis: v1 MVP Scope vs Current Implementation

To transition from Phase 0 scaffolding to a functioning v1 MVP, the following core architectural capabilities must be implemented:

```
[Target v1 MVP Architecture]                [Current Codebase State]
=======================================     =======================================
1. Docker Compose Infrastructure        vs  Empty docker-compose.yml
   - Postgres 16 (wal_level=logical)        (Docker daemon currently inactive)
   - Kafka (KRaft mode) + 4 topics
   - Debezium 2.5 + Outbox SMT
   - Redis 7.2 (Token bucket state)

2. Database Schema & Persistence        vs  Empty infra/postgres/init.sql
   - 6 Core relational tables               No schema migrations or DDL defined
   - Unique (event_id, subscription_id)
   - pgoutput logical replication user

3. Transactional Ingestion API           vs  Empty src/fanout/api/*
   - Atomic event + outbox ACID insert      No FastAPI routes or Pydantic schemas
   - Client idempotency key validation

4. Fan-Out Stream Engine                 vs  Empty src/fanout/fanout_worker/*
   - Consumer for events.raw                No subscription resolution cache
   - Delivery record generation & routing

5. Delivery Pipeline & Workers          vs  Empty src/fanout/delivery/*
   - Async HTTP webhook sender               Empty src/fanout/channels/*
   - HMAC-SHA256 signature generator
   - Exponential backoff with jitter
   - DLQ topic routing & DEAD state
   - Redis token bucket rate limiter

6. Verification & Load Test Harness     vs  Empty loadtest/* and tests/*
   - Programmable mock webhook receiver
   - Async load generator (200-1000 QPS)
   - Percentile latency & audit report
```

---

## 5. Technical Environment & Blocker Assessment

1. **Docker Daemon Availability:**
   - **Diagnosis:** Running `docker ps` returns:
     `failed to connect to the docker API at unix:///Users/shivenkathuria/.docker/run/docker.sock; dial unix /Users/shivenkathuria/.docker/run/docker.sock: connect: no such file or directory`.
   - **Impact:** Containers for PostgreSQL, Kafka, Kafka Connect (Debezium), and Redis cannot be booted until the host Docker Desktop / OrbStack service is started.
   - **Remediation:** Start Docker Desktop on macOS before executing `docker compose up -d` in Day 1.

2. **Python Environment & Dependencies:**
   - **Diagnosis:** Host Python version is `3.15.0` (`/opt/homebrew/bin/python3`). Fast package manager `uv` is available at `/Users/shivenkathuria/.local/bin/uv`. `pyproject.toml` is currently 0 bytes.
   - **Remediation:** Initialize `pyproject.toml` with project dependencies: `fastapi`, `uvicorn`, `sqlalchemy[asyncio]`, `asyncpg`, `aiokafka`, `httpx`, `redis`, `pydantic-settings`, `prometheus-client`, `pytest`, `pytest-asyncio`.

3. **PRD Architectural Decisions Status:**
   - **Q1 (Email Provider):** Resolved for v1 — use an abstract `EmailChannelAdapter` with `SMTPEmailAdapter` (connecting to a local MailHog/sink container) for automated CI/CD and correctness tests without third-party API dependencies.
   - **Q2 (Rate Limiting Storage):** Resolved for v1 — use Redis Token Bucket via atomic Lua scripts (`infra/redis/`) to support sub-millisecond rate checks without database contention.

---

## 6. Risk Register & Mitigation Strategy

| Risk ID | Risk Description | Severity | Probability | Mitigation Strategy |
|:---:|---|:---:|:---:|---|
| **RSK-01** | **Debezium & Logical Replication Configuration Failures** | High | High | Use the validated Debezium PostgreSQL Outbox Event Router SMT configuration (`infra/debezium/outbox-connector.json`). Verify LSN tracking and topic emission immediately on Day 1 before developing workers. |
| **RSK-02** | **Partition Stalls Under Failures (Option A HoL Blocking)** | High | Medium | Implement Option A blocking retries with a strict attempt ceiling (max 5 attempts, backoff capped at 30s) so dead destinations transition to DLQ swiftly. Implement clean worker interfaces to allow migration to Option C (parked keys). |
| **RSK-03** | **Rate Limiter Latency Bottlenecks** | Medium | Medium | Execute token-bucket evaluations atomically in Redis using Lua scripts. Implement in-memory sliding window fallback if Redis becomes unreachable. |
| **RSK-04** | **Benchmark Distortion from External Network Latency** | High | Low | Isolate throughput load tests strictly to the local high-performance `mock_receiver`. Restrict real email testing to isolated correctness runs. |
| **RSK-05** | **Poison Pill Payload Consumer Crashes** | Medium | Low | Enforce strict Pydantic validation at the Ingest API. Worker deserializers catch validation exceptions and route malformed records directly to `deliveries.dlq` without crashing consumer loops. |

---

## 7. Concrete Next Actions & Immediate Execution Sequence

### Phase 1: Infrastructure & Baseline Setup (Day 1)
1. **Host Environment:** Launch Docker Desktop / OrbStack to enable the local Docker daemon.
2. **Project Dependencies:** Populate `pyproject.toml` with FastAPI, SQLAlchemy Async, asyncpg, aiokafka, httpx, redis, pydantic-settings, and pytest; run `uv pip install -e .`.
3. **Database DDL:** Populate `infra/postgres/init.sql` with schema definitions for `subscribers`, `subscriptions`, `events`, `outbox`, `deliveries`, and `delivery_attempts` with indexes and logical replication grants.
4. **Compose Stack:** Populate `docker-compose.yml` configuring PostgreSQL 16 (logical WAL), Kafka KRaft broker, Kafka Connect with Debezium 2.5, and Redis 7.2.
5. **Topics & Connector:** Populate `infra/kafka/topics.sh` and `infra/debezium/outbox-connector.json`; execute verification gate confirming an outbox insert produces a message on `events.raw`.

### Phase 2: Ingestion & Fan-Out Implementation (Days 2 & 3)
1. Implement FastAPI application in `src/fanout/api/` with atomic outbox writing (`outbox.py`) and idempotency key caching.
2. Implement Fan-Out worker in `src/fanout/fanout_worker/worker.py` to consume `events.raw`, match active subscriptions, and dispatch to channel topics.

### Phase 3: Channel Delivery & Resiliency (Days 4 & 5)
1. Implement Webhook delivery worker with HMAC-SHA256 signature generation and response classification.
2. Implement exponential backoff retries, DLQ routing, and replay script (`scripts/replay_dlq.py`).
