# PRD: Notification Fan-Out Engine

Status: Draft
Owner: Shiven Kathuria

---

## 1. Problem

Developers building a new application — whether at a company or across a string of personal/portfolio projects — repeatedly re-implement the same notification logic: a direct call to an email or SMS provider's API, sprinkled into business-logic code wherever a message needs to go out. These ad hoc implementations typically have no retry behavior, no protection against sending the same message twice on retry, and no record of whether a message was actually delivered.

The cost shows up later, not at build time: a user reports they never received an OTP or a booking confirmation, and there is no way to tell whether it was never sent, sent and lost to a transient provider failure, or sent twice because a retry fired blindly. Each new project either accepts this risk or spends days rebuilding the same retry/idempotency/delivery-tracking logic from scratch — work that has nothing to do with that project's actual product.

This problem exists independent of any particular feature: it is a gap in how notification delivery is built, not a missing product capability.

## 2. Users

**Primary user:** A backend developer integrating notification delivery into an application's backend — the person who writes the code that calls "send this user an email/SMS/webhook" and who is on the hook when a delivery silently fails. This includes the author's own other projects, each of which currently needs this capability and would otherwise reimplement it independently.

**Secondary users:**
- The engine's own operator (initially, the author) — needs visibility into per-tenant usage, rate-limit activity, and the dead letter queue to keep the service healthy.
- End recipients of notifications — they don't interact with the engine directly, but delivery reliability and ordering determine whether what they experience (an OTP, a confirmation) is correct and timely.

**Excluded from this release:** Organizations needing a self-serve, multi-tenant SaaS product with signup, billing, and a management UI for non-technical users. This release is not optimized for that audience — it is a reconfigurable single-tenant service meant to be redeployed per project, not a hosted product serving unrelated third-party customers simultaneously.

## 3. Requirements

Each requirement below is a user-visible behavior with a condition, an expected outcome, and — where relevant — a failure path.

1. **Submitting a notification.** When a host app sends an authenticated request containing recipient details, one or both of the channels `email`/`sms` (or a `webhook` target), message content (template reference + variables, or raw content), and an idempotency key, the system responds with an accepted tracking ID without waiting for the message to actually be delivered.

2. **Idempotent resubmission.** If a request with an idempotency key already seen within the configured idempotency window is submitted again, the system returns the original tracking ID and result instead of dispatching a second notification. If the same key is reused with different payload content, the system rejects the request as a conflict rather than silently choosing one version.

3. **Per-channel delivery.** For each channel requested in a single submission, delivery is attempted and tracked independently. A host app can see that the email half of a request succeeded while the SMS half failed, rather than a single pass/fail result for the whole request.

4. **Transient failure retry.** If a channel's provider call fails with a transient error (e.g. timeout, 5xx), the system retries that channel's delivery with exponential backoff up to a configured attempt limit, without the host app needing to resubmit anything.

5. **Permanent failure handling.** If a channel's provider call fails with a non-retryable error (e.g. invalid phone number, hard email bounce), the system marks that channel's delivery as failed immediately and does not consume further retry attempts on it.

6. **Dead-lettering.** If retries are exhausted without success, the notification's status for that channel becomes `dead_lettered` and is visible via the status endpoint; it is not silently dropped.

7. **TTL expiry.** If a notification has a configured TTL and that TTL elapses before successful delivery on a channel, no further delivery attempt is made on that channel and its status becomes `expired` rather than `sent`.

8. **Delivery status query.** A host app can query a submitted request's tracking ID at any time and receive the current status (`pending`, `sent`, `failed`, `retrying`, `dead_lettered`, `expired`) per requested channel.

9. **Ordering per recipient.** Notifications targeting the same recipient key are processed in the order they were submitted, so a later state change (e.g. "cancelled") cannot be overtaken by an earlier one that was merely slow to process.

10. **Webhook signing.** Every webhook delivery is signed (HMAC) so the receiving endpoint can verify it originated from this engine and was not tampered with in transit.

11. **Secret rotation.** When a tenant's signing secret is rotated, requests signed with the previous secret continue to be accepted for a defined grace window, so rotation does not cause a hard outage for in-flight webhook deliveries.

12. **Rate limiting.** If a tenant or destination exceeds its configured rate limit, further sends are delayed (not dropped or errored to the caller) until capacity is available, and this delay is reflected as a `retrying`/`pending` status rather than a failure.

## 4. Non-goals

- **Multi-tenant self-serve SaaS features** (signup flow, billing, a non-technical admin UI) are explicitly out of scope for this release. The engine is reconfigurable per deployment, not a hosted product for unrelated external customers. *(Intentional exclusion.)*
- **A management dashboard/UI** for configuring templates, policies, and viewing the DLQ is not part of this release; configuration happens via the admin API or config files, and DLQ inspection happens via direct database/API queries. *(Deferred — may follow once the core engine is stable.)*
- **Exactly-once delivery to the external provider** is not guaranteed. The system provides idempotent, at-least-once delivery with idempotency keys honored where the provider supports them; a crash between "provider call made" and "status recorded" can in rare cases result in a duplicate send. *(Intentional, documented limitation — not solved in this release.)*
- **Push notifications** (mobile push, in-app) are not a supported channel in this release; only email, SMS, and outbound webhooks are. *(Deferred.)*
- **Analytics/reporting beyond raw per-notification status** (e.g. aggregate delivery-rate dashboards, cost reporting) is not included. *(Deferred.)*
- **Multi-recipient broadcast** (one event fanning out to many distinct recipients) is not a goal here; fan-out in this release means one notification fanning out across channels for a single recipient, not across many recipients. *(Intentional — distinct from a broadcast/campaign system.)*

---

## 5. Success metrics

- At least hundreds of events/sec sustained on the target load-test environment (Docker Compose, single-host) without error-rate degradation.
- Zero duplicate external-provider sends observed in load testing under normal (non-crash) operation.
- 100% of dead-lettered notifications are visible via the status/DLQ query path — none silently disappear.
- End-to-end latency from submission to provider call initiation stays within a defined bound (to be set once baseline load tests are run) under normal load.

## 6. Risks

- **Exactly-once is not achievable end-to-end** without provider-side idempotency support; this is a known, accepted risk rather than a solved problem (see Non-goals).
- **Secret rotation grace-window length** is not yet decided; too short risks breaking in-flight webhook deliveries, too long extends the exposure window for a compromised old secret.
- **DLQ growth without a replay mechanism** could become an operational blind spot if volume grows beyond what manual inspection can handle.
- **Config-read timing** (snapshot at enqueue vs. fresh read at dispatch) is an open design decision that affects correctness when a tenant changes policy mid-flight.

## 7. Launch constraints

- Target build window: approximately 10 days.
- Runs on Docker Compose on a single host for the initial release; not designed for this release to run across multiple hosts/clusters.
- Built with Python for the API and workers; Kafka for the pub/sub layer; PostgreSQL for the outbox and status store; Debezium for CDC relay; Twilio for SMS.

---

*Drafted against the Idea2PRD "PRD Bench" four-station framework (problem, users, requirements, non-goals). Technical interface/data-model specification is intentionally out of scope for this document and belongs in a separate technical spec.*
