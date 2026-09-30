# Notification Fan-Out Engine: 10-Day Task Plan

Assumes several focused hours per day. Debezium comes first because it is the riskiest infrastructure piece. Verify Debezium, Kafka client and migration-tool details against current docs before relying on them.

---

## Day 1: Infrastructure and schema
- [ ] Repo skeleton, `pyproject.toml`, Makefile, `.env.example`
- [ ] `docker-compose.yml`: Postgres, Kafka, Kafka Connect/Debezium
- [ ] Enable logical replication and create a replication-capable role
- [ ] Migrations for all tables (subscribers, subscriptions, events, outbox, deliveries, delivery_attempts)
- [ ] Create Kafka topics
- [ ] Register the Debezium outbox connector

**Done when:** manually inserting an outbox row makes a message appear on `events.raw`.

## Day 2: Ingest API
- [ ] `POST /events` writes `events` + `outbox` in one transaction
- [ ] Client idempotency key handling
- [ ] Subscription CRUD endpoints (admin-only for now)

**Done when:** one POST produces exactly one Kafka message, and a duplicate POST returns the original event.

## Day 3: Fan-out worker
- [ ] Consume `events.raw`, match active subscriptions by event type
- [ ] Insert `deliveries` rows (unique on event_id + subscription_id)
- [ ] Publish delivery messages to channel topics, keeping the ordering key
- [ ] Decide how to close the Postgres-then-Kafka gap (reprocess-on-restart vs second outbox)

**Done when:** replaying the same Kafka message creates no duplicate deliveries.

## Day 4: Webhook delivery
- [ ] Webhook channel: send, HMAC signature, event ID and delivery ID headers
- [ ] Status-code classification (success / retryable / terminal)
- [ ] Record `delivery_attempts`; commit offsets only after state is saved
- [ ] Basic mock receiver

**Done when:** an event reaches the mock receiver end to end with a valid signature.

## Day 5: Retries, DLQ and ordering (option A)
- [ ] Exponential backoff with jitter, max attempts
- [ ] DLQ topic and `dead` status
- [ ] Blocking retry per partition to preserve order
- [ ] DLQ inspect and replay (endpoint + script)

**Done when:** a receiver returning 500s ends up in the DLQ, and replay redelivers it.

## Day 6: Rate limiting
- [ ] Decide Redis vs Postgres for limiter state (PRD Q2)
- [ ] Per-destination limiter
- [ ] Limited deliveries are delayed, not failed

**Done when:** a destination with a low configured rate never exceeds it under a burst.

## Day 7: Email channel
- [ ] Choose the email provider (PRD Q1)
- [ ] Provider adapter behind the channel interface
- [ ] Transient vs permanent failure classification
- [ ] Small real-email correctness test (kept out of throughput runs)

**Done when:** a real email is delivered, and a bad address is marked terminal.

## Day 8: Observability and load-test harness
- [ ] Metrics: consumer lag, delivery latency, attempt outcomes, DLQ depth
- [ ] Delivery status query endpoint
- [ ] Load generator with a target event rate and fan-out factor
- [ ] Configurable mock receiver (latency, error rate)
- [ ] Report script: throughput, p50/p95/p99 latency, duplicate rate

**Done when:** one command runs a scenario and prints a report.

## Day 9: Benchmarks and fault injection
- [ ] Baseline runs
- [ ] Scenarios: receiver returning 500s, slow receiver, worker killed mid-run
- [ ] Check duplicate rate and per-key ordering
- [ ] Tune batch sizes, pool sizes, partition counts
- [ ] Save results and hardware notes in `docs/benchmarks/`

**Done when:** you have numbers you can defend, measured on your machine.

## Day 10: Stretch and polish
- [ ] Stretch: parked-key ordering (option C), only if days 1 to 9 went smoothly; otherwise document it as future work
- [ ] Test cleanup (unit, integration, ordering)
- [ ] README and architecture diagram
- [ ] Write-up of results and trade-offs

---

## Likely slip points
- **Day 1:** Debezium setup can be stubborn.
- **Day 5:** ordering can interact badly with retries, which is why option A comes first.
- **Day 9:** load tests often expose bugs; Day 10 is the buffer.

## If you need to cut scope
1. Drop parked-key ordering.
2. Drop the real-provider email test.
3. Keep the benchmark; it is the part that distinguishes the project.
