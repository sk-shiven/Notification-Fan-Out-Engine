# Notification Fan-Out Engine: PRD (v0.1)

**Status:** Draft · **Scope:** Portfolio demo with a reproducible load-test benchmark

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