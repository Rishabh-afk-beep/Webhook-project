# Reliable Webhook Delivery Platform — Architecture & Reliability Audit Report

**Date:** October 30, 2026  
**Auditor:** Senior Backend & Reliability-Testing Engineer  
**Codebase:** Reliable Webhook Delivery Platform (FastAPI, PostgreSQL, Redis, Celery, Prometheus, Grafana, k6)

---

## 1. Executive Summary

A comprehensive architectural and reliability audit was conducted across the Reliable Webhook Delivery Platform codebase. The audit inspected database schemas, Alembic migrations, service layers, outbox dispatcher, Celery worker task execution, failure-injection mechanisms, and monitoring pipelines.

The platform demonstrates a solid architectural foundation by employing the **Transactional Outbox Pattern** to prevent the dual-write problem between PostgreSQL and Redis, enforcing multi-tenant data isolation, utilizing row-level locks (`SELECT FOR UPDATE SKIP LOCKED`), implementing exponential backoff with jitter, dead-letter queues, Redis-based token bucket rate limiting, and Redis-backed circuit breaking.

However, the audit identified **one critical reliability defect** (stuck deliveries on worker crash during `processing`), an architectural gap in **event ingestion idempotency**, and limitations in the **k6 performance benchmarking suite** which currently only evaluates synchronous API ingestion rather than end-to-end asynchronous delivery.

---

## 2. Verified Functionality

| Capability | Implementation File(s) | Verification Status | Notes |
|---|---|---|---|
| **API Key Authentication** | `app/api/auth.py`, `app/core/security.py` | Verified | High-entropy SHA-256 hashed keys; constant-time verification; `wh_live_` prefix. |
| **Tenant Isolation** | `app/models/*`, `app/api/*`, `app/services/*` | Verified | Every query scopes by `tenant_id`. Cross-tenant destination and event access tests pass. |
| **Transactional Outbox** | `app/services/event_service.py`, `app/models/outbox.py` | Verified | `Event`, `EventDelivery`, and `OutboxEntry` created in a single PostgreSQL transaction. |
| **Outbox Dispatcher** | `app/dispatcher/outbox_dispatcher.py`, `app/services/outbox_service.py` | Verified | Uses `SELECT ... FOR UPDATE SKIP LOCKED` for concurrent batch claiming. Marks `published` after Redis accept. |
| **Asynchronous Delivery** | `app/workers/tasks.py`, `app/workers/celery_app.py` | Verified | Celery task with row-level locking (`with_for_update()`) and HMAC-SHA256 signature generation. |
| **Retries & Exponential Backoff** | `app/workers/retry_policy.py`, `app/workers/tasks.py` | Verified | Exponential backoff $2.0 \times 2^{attempt-1} \times jitter$; retryable errors (408, 429, 5xx, timeouts, connect errors) scheduled via Celery countdown without worker sleep. |
| **Dead-Letter Queue (DLQ)** | `app/services/dlq_service.py`, `app/api/dead_letters.py` | Verified | Transitions to `dead_lettered` after 5 attempts; paginated tenant-scoped DLQ discovery API. |
| **Manual Replay** | `app/services/dlq_service.py`, `app/api/dead_letters.py` | Verified | Creates linked child delivery (`replay_of_delivery_id`); preserves original attempt history; double-replay blocked with 409 Conflict. |
| **Distributed Rate Limiting** | `app/core/rate_limiter.py` | Verified | Redis Lua token bucket; calculates deficit delay; defers without fake HTTP attempts; fails open on Redis error. |
| **Distributed Circuit Breaker** | `app/core/circuit_breaker.py` | Verified | Redis state machine: `CLOSED` $\rightarrow$ `OPEN` $\rightarrow$ `HALF_OPEN` $\rightarrow$ `CLOSED`; trips on 5 failures; defers without fake HTTP attempts; fails open. |
| **Prometheus Metrics** | `app/core/metrics.py`, `app/api/metrics.py` | Verified | Low-cardinality counters, histogram, and operational gauges synced from PostgreSQL and Redis on `/metrics` scrape. |
| **Grafana Dashboards** | `monitoring/grafana/provisioning/...` | Verified | Provisioned dashboard with 12 stat and timeseries panels at `http://localhost:3000`. |

---

## 3. Existing Test Coverage

The test suite currently contains **104 automated tests** across 14 test modules in `tests/`:

- `test_authentication.py` (4 tests): Missing, invalid, valid, and revoked API keys.
- `test_destinations.py` (7 tests): CRUD operations, SSRF private IP validation, loopback blocking, pagination, tenant isolation.
- `test_event_ingestion.py` (10 tests): Single & multi-destination ingestion, empty destinations, nonexistent, cross-tenant destinations, inactive destination, invalid JSON payload, get event.
- `test_delivery_status.py` (4 tests): Delivery retrieval, 404 handling, cross-tenant boundary, empty attempts list.
- `test_delivery_worker.py` (11 tests): 200 OK, 500 server error retry, 429 rate limit retry, connect error, timeout, 400 bad request non-retryable failure, attempt history audit recording, duplicate task discard, nonexistent delivery, inactive destination, HMAC signing.
- `test_outbox.py` (8 tests): Outbox row creation, rollback atomicity, batch claiming with SKIP LOCKED, mark published, error keeps pending, batch success/failure.
- `test_health.py` (2 tests): Liveness (`/health`) and database readiness (`/health/ready`).
- `test_integration.py` (7 tests): End-to-end event lifecycle, atomic persistence, tenant boundaries.
- `test_retry_policy.py` (15 tests): Status code classification, exception classification, exponential curve calculation, jitter bounds, error categorization, worker retry execution, max attempts dead-lettering, duplicate task after retry.
- `test_dlq.py` (9 tests): Empty DLQ, pagination, cross-tenant DLQ isolation, get by ID, unauthorized replay, successful replay, concurrent replay locking, ineligible delivery replay rejection, cross-tenant replay rejection.
- `test_rate_limiter.py` (6 tests): Requests under limit allowed, exceeding limit delayed, destination independence, token replenishment, Redis failure fail-open, worker deferral without fake attempt.
- `test_circuit_breaker.py` (8 tests): Starts closed, trips to open on 5 failures, non-qualifying 4xx doesn't trip, half-open transition, probe closes circuit, probe failure re-opens, destination independence, worker deferral without fake attempt.
- `test_metrics.py` (3 tests): Endpoint accessible, counter increments on event ingestion, sensitive label exclusion.
- `test_failure_injection.py` (10 tests): HTTP 500, 429, timeout, connection refused, Redis unavailable fail-open, duplicate task guard, concurrent worker guard, double-replay guard, transaction rollback, dispatcher crash before mark published.

**Live Execution Result:** All 104 tests were executed against Python 3.13 and passed in `2.55s`.

---

## 4. Failure Mode Analysis (Component Unavailability)

| Failed Component | Failure Stage | Expected System Behavior | Actual Observed Behavior | Durability / Loss Risk |
|---|---|---|---|---|
| **API Container** | Before DB commit | Transaction rolls back; client receives 500/timeout | Ingestion transaction rolls back cleanly; no orphan records | **Zero loss** (client must retry) |
| **API Container** | After DB commit, before 202 response | Event & outbox are committed in PostgreSQL | Outbox dispatcher picks up and delivers event; client may retry | **Duplicate risk** (no API idempotency key) |
| **PostgreSQL** | Ingestion | API cannot write event/outbox; returns 500 | Ingestion fails; client receives 500 | **Zero loss** (acknowledged failure) |
| **PostgreSQL** | Outbox polling | Dispatcher cannot query outbox; logs error | Dispatcher retries on next poll interval | **Zero loss** |
| **PostgreSQL** | Worker delivery | Worker cannot update delivery/attempts | Worker rolls back; Celery redelivers task (`acks_late=True`) | **Zero loss**, potential duplicate HTTP if crash was post-HTTP |
| **Redis** | Ingestion | API only talks to PostgreSQL | Ingestion completely unaffected (HTTP 202 returned) | **Zero loss** (Massive strength of Outbox) |
| **Redis** | Outbox dispatch | Dispatcher fails to publish task | Logs error, leaves outbox `status='pending'`, retries on next poll | **Zero loss** |
| **Redis** | Rate limiter / Circuit breaker | Redis commands raise ConnectionError | Fail-open safely with warning; deliveries proceed | **Zero loss** (prioritizes delivery over throttling) |
| **Worker** | Before HTTP request | Delivery marked `processing`; worker crashes | Celery redelivers task, but worker rejects it as `"already_processing"`! | **CRITICAL BUG: Delivery stuck in `processing` permanently** |
| **Worker** | After remote HTTP 200, before DB commit | Remote server processed event; local worker died | Celery redelivers task; worker may retry and send 2nd HTTP request | **At-least-once reality: duplicate remote HTTP request** |
| **Worker** | During retry countdown | Celery holds delayed task in Redis broker | Redis/Celery preserves scheduled ETA; delivery resumes on worker restart | **Zero loss** |

---

## 5. Potential Reliability Bugs & Architecture Gaps

### Bug 1: Stuck `processing` Deliveries upon Worker Crash (Severity: CRITICAL)
- **Location:** `app/workers/tasks.py` (lines 48, 96-109, 112-113).
- **Mechanism:** When a worker starts processing a delivery, it commits `delivery.status = "processing"` and releases the row lock. If the worker container crashes, segfaults, or is terminated by OOM killer before completing the HTTP request or committing the outcome:
  1. Celery has `task_acks_late=True`, so the unacknowledged task is redelivered to a worker upon restart.
  2. The restarted worker inspects `delivery.status`:
     `if delivery.status not in ELIGIBLE_STATUSES:` (where `ELIGIBLE_STATUSES = {"pending", "retry_scheduled"}`).
  3. Because `delivery.status == "processing"`, it matches `"already_processing"`.
  4. The worker logs `"Delivery not eligible for processing - discarding"` and **permanently discards the task**.
  5. The delivery remains stranded in `status = "processing"` forever. It is never retried, never dead-lettered, and never completed.
- **Root Cause:** Releasing the database row lock after setting `processing` without an active processing lease / timeout recovery mechanism.

### Bug 2: Missing Event Ingestion Idempotency Key (Severity: HIGH)
- **Location:** `app/models/event.py`, `app/api/events.py`, `app/services/event_service.py`.
- **Mechanism:** Neither the `events` table nor the `POST /v1/events` endpoint supports an `idempotency_key` or `Idempotency-Key` HTTP header.
- **Impact:** In distributed networks, client timeouts or connection resets frequently occur after the server commits the transaction but before the client receives the HTTP 202 response. When the client retries the request, two separate `events` rows, two sets of `event_deliveries`, and two `outbox_entries` are created, causing uncontrolled duplicate webhook deliveries.

### Bug 3: Test Receiver In-Memory Storage Eviction (Severity: MEDIUM)
- **Location:** `test_receiver/main.py` (lines 40-42).
- **Mechanism:** `_received_requests` is hard-capped at 1,000 requests (`_received_requests.pop(0)`).
- **Impact:** During load testing at 100 to 500 VUs where several thousand events are delivered, earlier request records are evicted, causing post-load verification assertions to falsely report missing deliveries.

### Bug 4: Database Connection Pool Sizing under High Concurrency (Severity: MEDIUM)
- **Location:** `app/db/session.py` (lines 21-27).
- **Mechanism:** Engine configured with `pool_size=10, max_overflow=20` (maximum 30 connections).
- **Impact:** When subjected to 250–500 concurrent virtual users during stress testing, API requests will queue or timeout waiting for database connection checkouts unless connection pooling is expanded or concurrency is throttled.

---

## 6. Performance-Testing Limitations of Current k6 Suite

1. **Only Tests Synchronous API Acceptance:** The existing k6 scenarios (`event_ingestion.js`, `increasing_load.js`, `mixed_load.js`, `multi_destination.js`) only measure HTTP 202 response times on `POST /v1/events`. They do not track whether the outbox dispatcher published the work, whether Celery workers delivered the requests, or what the end-to-end delivery latency was.
2. **Limited Virtual User Scale:** Previous runs only executed 10–50 VUs for 10–30s. No benchmarks exist for 100, 250, or 500 concurrent VUs.
3. **No Queue Drain Verification:** The existing k6 suite does not verify whether the Celery broker queue and PostgreSQL outbox table drain to zero after the load burst completes.
4. **Static Payloads & Single Destinations:** Most scenarios used fixed payloads without varying destination counts, failing to benchmark fan-out delivery performance under load.

---

## 7. Recommended Action Plan & Priority Ranking

| Priority | Action Item | Target Phase |
|---|---|---|
| **P1 (Critical)** | Implement a Processing Lease / Timeout Recovery mechanism in `tasks.py` to allow redelivered tasks from crashed workers to be safely reclaimed and executed. | Phase 2 |
| **P2 (Critical)** | Build an automated 100-scenario Worker Crash & Recovery test suite exercising all 7 failure points with deterministic failure injection and database verification. | Phase 2 |
| **P3 (High)** | Add optional `idempotency_key` support on `events` with a unique constraint `(tenant_id, idempotency_key)` and HTTP header support in `POST /v1/events`. | Phase 2 / Fixes |
| **P4 (Medium)** | Upgrade `test_receiver` to maintain cumulative atomic counters alongside recent request logs to support high-throughput load testing without eviction loss. | Phase 3 |
| **P5 (Medium)** | Expand the k6 load testing suite to support phased execution (5 $\rightarrow$ 20 $\rightarrow$ 50 $\rightarrow$ 100 $\rightarrow$ 250 $\rightarrow$ 500 VUs) measuring both API ingestion and asynchronous delivery completion. | Phase 3 |
| **P6 (Medium)** | Verify Prometheus metrics accuracy under load, ensuring counters increment exactly once and histogram buckets match observed latency percentiles. | Phase 4 |
| **P7 (Low)** | Produce the comprehensive final verification report and machine-readable test artifacts in `reports/`. | Phase 5 |

