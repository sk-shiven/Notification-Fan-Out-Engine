# Notification Fan-Out Engine

A high-reliability, distributed notification delivery system built on the **Transactional Outbox Pattern with Change Data Capture (CDC)**.

## Architecture Highlights
- **Atomic Ingestion**: Event + outbox write within a single PostgreSQL ACID transaction.
- **Debezium CDC**: PostgreSQL WAL logical replication streaming outbox records directly into Kafka (`events.raw`).
- **Idempotent Fan-Out**: Event-to-subscription resolution with composite uniqueness constraint `(event_id, subscription_id)`.
- **Multi-Channel Dispatch**: Outbound delivery workers for Webhooks (HMAC-SHA256 signatures, connection pooling) and Email (SMTP / SES adapters).
- **Traffic Shaping & Reliability**: Redis Token Bucket rate limiting, exponential backoff retries with full jitter, DLQ routing, and per-key causal ordering.
- **Observability & Benchmarks**: Prometheus metrics and empirical benchmarking harness measuring throughput, latency percentiles (p50/p95/p99), and duplicate rates.

## Development Setup

### Prerequisites
- macOS or Linux
- Python >= 3.11
- [uv](https://github.com/astral-sh/uv) (fast Python package and project manager)
- Docker Desktop or Docker engine

### Quickstart

1. **Start Docker:**
   ```bash
   ./scripts/start_docker.sh
   ```

2. **Initialize Python Environment:**
   ```bash
   uv venv --python 3.13
   uv pip install -e ".[dev]"
   ```

3. **Verify Environment:**
   ```bash
   .venv/bin/python -c "import fanout; print('fanout package loaded successfully')"
   ```
