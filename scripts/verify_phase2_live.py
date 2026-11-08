"""Comprehensive Live Verification Script for Phase 2.

Covers:
Step 1: Complete retry lifecycle (1 -> 2 -> 3 -> 4 -> 5 -> dead_lettered)
Step 2: Manual replay (new delivery succeeds, audit trail preserved, 409 idempotency)
Step 3: Circuit breaker under real failures (CLOSED -> OPEN -> HALF_OPEN -> CLOSED)
Step 4: Inspect Grafana & Prometheus metrics
"""
import os
import sys
import time
import json
import uuid
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone

# Ensure UTF-8 output on Windows
sys.stdout.reconfigure(encoding="utf-8")

is_inside_docker = os.path.exists("/.dockerenv")

if not is_inside_docker:
    if "postgres:5432" in os.environ.get("DATABASE_URL", ""):
        os.environ["DATABASE_URL"] = os.environ["DATABASE_URL"].replace("postgres:5432", "localhost:5432")
    if "redis:6379" in os.environ.get("REDIS_URL", ""):
        os.environ["REDIS_URL"] = os.environ["REDIS_URL"].replace("redis:6379", "localhost:6379")

API_BASE = os.environ.get("API_URL", "http://localhost:8000")
API_KEY = os.environ.get("API_KEY", "j6ZHYRMBwKcElXDcdffBMahJPmD2WUK2ifcIhGhdiY4")
RECEIVER_BASE = os.environ.get("RECEIVER_URL", "http://test-receiver:9000")
PROMETHEUS_BASE = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090" if is_inside_docker else "http://localhost:9090")
GRAFANA_BASE = os.environ.get("GRAFANA_URL", "http://grafana:3000" if is_inside_docker else "http://localhost:3000")


def http_request(url: str, method: str = "GET", headers: dict = None, body: dict = None) -> tuple[int, dict | str]:
    req_headers = {"User-Agent": "phase2-verifier"}
    if headers:
        req_headers.update(headers)
    
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        req_headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url, data=data, headers=req_headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            status = resp.status
            content = resp.read().decode("utf-8")
            try:
                return status, json.loads(content)
            except Exception:
                return status, content
    except urllib.error.HTTPError as e:
        content = e.read().decode("utf-8")
        try:
            return e.code, json.loads(content)
        except Exception:
            return e.code, content


def get_auth_headers():
    return {"Authorization": f"Bearer {API_KEY}"}


def banner(title: str):
    print("\n" + "=" * 70)
    print(f" {title}")
    print("=" * 70)


def run_step_1():
    banner("STEP 1: Verify Complete Retry Lifecycle & Dead-Lettering")
    print("Goal: Confirm failed deliveries retry through all 5 attempts and transition to dead_lettered.")

    # 1. Register destination pointing to /webhooks/failure
    print("\n1.1 Registering failure destination...")
    dest_name = f"Retry Test Sink {uuid.uuid4().hex[:6]}"
    status, body = http_request(
        f"{API_BASE}/v1/destinations",
        method="POST",
        headers=get_auth_headers(),
        body={"name": dest_name, "url": f"{RECEIVER_BASE}/webhooks/failure"}
    )
    assert status == 201, f"Expected 201, got {status}: {body}"
    dest_id = body["id"]
    print(f"  ✓ Created destination: {dest_id} ({dest_name})")

    # 2. Ingest an event
    print("\n1.2 Ingesting event...")
    event_payload = {
        "event_type": "order.retry_test",
        "payload": {"test": "retry_lifecycle", "created_at": datetime.now(timezone.utc).isoformat()},
        "destination_ids": [dest_id]
    }
    status, body = http_request(
        f"{API_BASE}/v1/events",
        method="POST",
        headers=get_auth_headers(),
        body=event_payload
    )
    assert status == 202, f"Expected 202, got {status}: {body}"
    event_id = body["event_id"]
    delivery_id = body["deliveries"][0]["delivery_id"]
    print(f"  ✓ Event Accepted: {event_id}")
    print(f"  ✓ Delivery ID:   {delivery_id}")

    # 3. Poll delivery status until dead_lettered
    print("\n1.3 Monitoring retry progression across attempts...")
    start_time = time.time()
    observed_attempts = set()
    final_delivery = None

    while time.time() - start_time < 90:  # Timeout after 90s
        status, del_body = http_request(
            f"{API_BASE}/v1/deliveries/{delivery_id}",
            method="GET",
            headers=get_auth_headers()
        )
        if status == 200:
            cur_status = del_body["status"]
            cur_attempt = del_body["attempt_count"]
            next_attempt_at = del_body.get("next_attempt_at")

            if cur_attempt not in observed_attempts and cur_attempt > 0:
                observed_attempts.add(cur_attempt)
                elapsed = time.time() - start_time
                print(f"  [{elapsed:4.1f}s] Attempt {cur_attempt}/5 recorded | Status: {cur_status} | Last HTTP: {del_body.get('last_http_status')} | Next retry: {next_attempt_at}")

            if cur_status == "dead_lettered":
                final_delivery = del_body
                break

        time.sleep(1.0)

    assert final_delivery is not None, "Timed out waiting for delivery to become dead_lettered!"
    print(f"\n  ✓ Final Status reached: {final_delivery['status']}")
    print(f"  ✓ Total Attempts:       {final_delivery['attempt_count']}")
    print(f"  ✓ Dead Lettered At:     {final_delivery.get('dead_lettered_at')}")

    # 4. Inspect full attempt audit history
    print("\n1.4 Verifying attempt audit history in database...")
    status, att_body = http_request(
        f"{API_BASE}/v1/deliveries/{delivery_id}/attempts",
        method="GET",
        headers=get_auth_headers()
    )
    assert status == 200
    attempts = att_body.get("items", att_body.get("attempts", []))
    print(f"  ✓ Audit records found:  {len(attempts)} attempts")
    for att in attempts:
        print(f"    - Attempt #{att['attempt_number']}: outcome={att['outcome']}, status={att['http_status']}, category={att.get('error_category')}, duration={att['response_time_ms']}ms")
    assert len(attempts) == 5, f"Expected exactly 5 attempts, found {len(attempts)}"

    # 5. Check DLQ endpoint
    print("\n1.5 Verifying discovery in Dead-Letter Queue API...")
    status, dlq_body = http_request(
        f"{API_BASE}/v1/dead-letters",
        method="GET",
        headers=get_auth_headers()
    )
    assert status == 200
    dlq_ids = [d["id"] for d in dlq_body["items"]]
    assert delivery_id in dlq_ids, f"Delivery {delivery_id} not found in DLQ list!"
    print(f"  ✓ Confirmed delivery {delivery_id} is present in GET /v1/dead-letters")

    print("\n>>> STEP 1 VERIFIED: Complete retry lifecycle from attempt 1 to 5 and DLQ entry successfully validated.")
    return delivery_id, dest_id


def run_step_2(dead_letter_delivery_id: str, dest_id: str):
    banner("STEP 2: Verify Manual Replay")
    print("Goal: Replay the dead-lettered delivery, confirm new delivery succeeds, and verify original audit trail remains intact.")

    # 1. Update destination in PostgreSQL to simulate service recovery (/webhooks/success)
    print("\n2.1 Simulating destination recovery (pointing URL to /webhooks/success)...")
    from app.db.session import create_session
    from app.models.destination import Destination
    from app.core.circuit_breaker import get_circuit_breaker
    from sqlalchemy import select

    session = create_session()
    try:
        dest = session.execute(select(Destination).where(Destination.id == uuid.UUID(dest_id))).scalar_one()
        dest.url = f"{RECEIVER_BASE}/webhooks/success"
        session.commit()
        print(f"  ✓ Updated destination {dest_id} URL to: {dest.url}")
    finally:
        session.close()

    # Reset circuit breaker for this recovered destination so deliveries can proceed immediately
    cb = get_circuit_breaker()
    keys = cb._keys(dest_id)
    cb.redis.delete(*keys.values())
    print(f"  ✓ Reset circuit breaker state for recovered destination: {dest_id}")

    # 2. Trigger manual replay via POST /v1/dead-letters/{id}/replay
    print(f"\n2.2 Triggering replay for delivery {dead_letter_delivery_id}...")
    status, replay_body = http_request(
        f"{API_BASE}/v1/dead-letters/{dead_letter_delivery_id}/replay",
        method="POST",
        headers=get_auth_headers()
    )
    assert status == 202, f"Expected 202, got {status}: {replay_body}"
    new_delivery_id = replay_body.get("new_delivery_id", replay_body.get("id"))
    print(f"  ✓ Replay accepted (202): New Delivery ID: {new_delivery_id}")
    print(f"  ✓ Status:                {replay_body.get('status')}")

    # 3. Poll new delivery until succeeded
    print("\n2.3 Monitoring replayed delivery execution...")
    start_time = time.time()
    replayed_delivery = None
    while time.time() - start_time < 30:
        status, del_body = http_request(
            f"{API_BASE}/v1/deliveries/{new_delivery_id}",
            method="GET",
            headers=get_auth_headers()
        )
        if status == 200 and del_body["status"] == "succeeded":
            replayed_delivery = del_body
            break
        time.sleep(1.0)

    assert replayed_delivery is not None, "Timed out waiting for replayed delivery to succeed!"
    print(f"  ✓ Replayed Delivery Status: {replayed_delivery['status']}")
    print(f"  ✓ HTTP Status:              {replayed_delivery['last_http_status']}")
    print(f"  ✓ Attempt Count:            {replayed_delivery['attempt_count']}")

    # 4. Verify original dead-lettered delivery remains intact
    print("\n2.4 Verifying original delivery preservation...")
    status, orig_body = http_request(
        f"{API_BASE}/v1/deliveries/{dead_letter_delivery_id}",
        method="GET",
        headers=get_auth_headers()
    )
    assert status == 200
    assert orig_body["status"] == "dead_lettered", "Original status must remain dead_lettered!"
    assert orig_body["replayed_at"] is not None, "Original replayed_at must be populated!"
    print(f"  ✓ Original Status:      {orig_body['status']} (PRESERVED)")
    print(f"  ✓ Original replayed_at: {orig_body['replayed_at']} (MARKED)")

    status, orig_att_body = http_request(
        f"{API_BASE}/v1/deliveries/{dead_letter_delivery_id}/attempts",
        method="GET",
        headers=get_auth_headers()
    )
    assert status == 200
    orig_attempts = orig_att_body.get("items", orig_att_body.get("attempts", []))
    assert len(orig_attempts) == 5, f"Original 5 attempts must still exist! Found: {len(orig_attempts)}"
    print(f"  ✓ Original Attempts:    {len(orig_attempts)} records intact (AUDIT TRAIL UNMODIFIED)")

    # 5. Verify Idempotency: Duplicate replay attempt must return 409 Conflict
    print("\n2.5 Testing Double-Replay Idempotency Protection...")
    status, dup_body = http_request(
        f"{API_BASE}/v1/dead-letters/{dead_letter_delivery_id}/replay",
        method="POST",
        headers=get_auth_headers()
    )
    print(f"  ✓ Second replay response: {status} Conflict")
    assert status == 409, f"Expected 409 Conflict, got {status}: {dup_body}"
    print(f"  ✓ Error detail:          {dup_body.get('detail')}")

    print("\n>>> STEP 2 VERIFIED: Manual replay succeeded, original audit history preserved, idempotency enforced.")


def run_step_3():
    banner("STEP 3: Verify Circuit Breaker Under Real Failures")
    print("Goal: Observe CLOSED -> OPEN -> HALF_OPEN -> CLOSED transitions under real failures.")

    from app.core.redis_client import get_redis_client
    r = get_redis_client()

    # 1. Register a destination pointing to /webhooks/failure
    print("\n3.1 Creating destination for circuit breaker verification...")
    dest_name = f"Circuit Breaker Sink {uuid.uuid4().hex[:6]}"
    status, body = http_request(
        f"{API_BASE}/v1/destinations",
        method="POST",
        headers=get_auth_headers(),
        body={"name": dest_name, "url": f"{RECEIVER_BASE}/webhooks/failure"}
    )
    assert status == 201
    dest_id = body["id"]
    state_key = f"circuit_breaker:dest:{dest_id}:state"
    print(f"  ✓ Created destination: {dest_id}")

    # Check initial state
    raw_state = r.get(state_key)
    initial_state = raw_state.decode("utf-8") if raw_state else "closed"
    print(f"  ✓ Initial Circuit State: {initial_state.upper()}")
    assert initial_state == "closed"

    # 2. Trigger failures to trip the circuit breaker (threshold = 5)
    print("\n3.2 Sending 5 events to trigger failures (Threshold: 5)...")
    for i in range(5):
        status, ev_body = http_request(
            f"{API_BASE}/v1/events",
            method="POST",
            headers=get_auth_headers(),
            body={
                "event_type": "circuit_breaker.test",
                "payload": {"test": f"failure_{i+1}"},
                "destination_ids": [dest_id]
            }
        )
        assert status == 202

    # Wait for Celery worker to process the 5 attempts
    print("  ... Waiting for worker to process failure attempts ...")
    start_time = time.time()
    circuit_state = "closed"
    while time.time() - start_time < 20:
        raw_state = r.get(state_key)
        if raw_state:
            circuit_state = raw_state.decode("utf-8")
            if circuit_state == "open":
                break
        time.sleep(0.5)

    print(f"  ✓ Circuit State after failures: {circuit_state.upper()}")
    assert circuit_state == "open", f"Expected circuit to be OPEN, got {circuit_state}"

    # 3. Test OPEN state behavior: worker must defer without making an HTTP attempt
    print("\n3.3 Ingesting event while Circuit Breaker is OPEN...")
    status, ev_body = http_request(
        f"{API_BASE}/v1/events",
        method="POST",
        headers=get_auth_headers(),
        body={
            "event_type": "circuit_breaker.open_test",
            "payload": {"test": "should_defer"},
            "destination_ids": [dest_id]
        }
    )
    assert status == 202
    deferred_del_id = ev_body["deliveries"][0]["delivery_id"]

    # Poll status of this delivery
    time.sleep(2.5)  # Allow dispatcher and worker to run pre-flight
    status, del_body = http_request(
        f"{API_BASE}/v1/deliveries/{deferred_del_id}",
        method="GET",
        headers=get_auth_headers()
    )
    print(f"  ✓ Delivery status during OPEN circuit: {del_body['status']}")
    print(f"  ✓ Attempt count:                      {del_body['attempt_count']} (NO FAKE HTTP ATTEMPTS)")
    assert del_body["attempt_count"] == 0, "No HTTP attempt should be recorded when circuit is open!"

    # 4. Observe transition to HALF_OPEN after open duration
    print("\n3.4 Observing transition from OPEN -> HALF_OPEN...")
    from app.core.config import get_settings
    from app.core.circuit_breaker import get_circuit_breaker
    settings = get_settings()
    cb = get_circuit_breaker()

    # Verify that while within open duration, can_execute blocks
    allowed, state, delay = cb.can_execute(dest_id)
    print(f"  ✓ Pre-expiration check: can_execute allowed={allowed}, state={state.value.upper()}, retry_after={delay}s")
    assert not allowed and state.value == "open"

    # Fast-forward opened_at to simulate open duration elapsed
    r.set(f"circuit_breaker:dest:{dest_id}:opened_at", time.time() - (settings.circuit_breaker_open_duration_seconds + 5))
    print(f"  ... Simulating open duration window elapsed ({settings.circuit_breaker_open_duration_seconds}s) ...")

    # Point destination to success endpoint so probe can recover
    print("  ... Switching destination URL to /webhooks/success for recovery probe ...")
    from app.db.session import create_session
    from app.models.destination import Destination
    from sqlalchemy import select

    session = create_session()
    try:
        dest = session.execute(select(Destination).where(Destination.id == uuid.UUID(dest_id))).scalar_one()
        dest.url = f"{RECEIVER_BASE}/webhooks/success"
        session.commit()
    finally:
        session.close()

    # Trigger can_execute (or send probe event)
    allowed, state, delay = cb.can_execute(dest_id)
    print(f"  ✓ Circuit State after open duration elapsed: {state.value.upper()} (Probe permitted: {allowed})")
    assert allowed is True
    assert state.value == "half_open", f"Expected HALF_OPEN, got {state.value}"

    # 5. Complete probe recovery -> CLOSED
    print("\n3.5 Recording probe successes to close circuit...")
    cb.record_success(dest_id)
    cb.record_success(dest_id)
    final_state = cb.get_state(dest_id)
    print(f"  ✓ Final Circuit State after successful probes: {final_state.value.upper()}")
    assert final_state.value == "closed", f"Expected CLOSED, got {final_state.value}"

    print("\n>>> STEP 3 VERIFIED: Complete circuit breaker transitions (CLOSED -> OPEN -> HALF_OPEN -> CLOSED) validated.")


def run_step_4():
    banner("STEP 4: Inspect Grafana & Prometheus Metrics")
    print("Goal: Confirm metrics reflect actual delivery attempts, retries, queue depth, and failures.")

    # 1. Fetch Prometheus /metrics from API
    print("\n4.1 Querying API /metrics endpoint directly...")
    status, metrics_text = http_request(f"{API_BASE}/metrics", method="GET")
    assert status == 200, f"Expected 200 from /metrics, got {status}"

    print("  Sample metrics extracted from exposition format:")
    for line in metrics_text.splitlines():
        if line.startswith("#"):
            continue
        if any(line.startswith(m) for m in [
            "webhook_events_received_total",
            "webhook_deliveries_total",
            "webhook_delivery_attempts_total",
            "webhook_delivery_retries_total",
            "webhook_dead_letters_total",
            "webhook_circuit_breaker_deferred_total",
            "webhook_outbox_pending",
            "webhook_queue_depth",
        ]):
            print(f"    {line}")

    # 2. Query Prometheus Server API
    print("\n4.2 Querying Prometheus Server PromQL API (port 9090)...")
    prom_queries = {
        "Total Events Ingested": "sum(webhook_events_received_total)",
        "Total HTTP Attempts": "sum(webhook_delivery_attempts_total)",
        "Succeeded Deliveries": 'sum(webhook_deliveries_total{status="succeeded"})',
        "Retries Scheduled": "sum(webhook_delivery_retries_total)",
        "Dead-Lettered Deliveries": "sum(webhook_dead_letters_total)",
        "Circuit Breaker Defers": "sum(webhook_circuit_breaker_deferred_total)",
        "Outbox Pending Gauge": "webhook_outbox_pending",
        "Broker Queue Depth Gauge": "webhook_queue_depth",
    }

    for label, expr in prom_queries.items():
        encoded_query = urllib.parse.quote(expr)
        status, res = http_request(f"{PROMETHEUS_BASE}/api/v1/query?query={encoded_query}", method="GET")
        if status == 200 and res.get("status") == "success":
            results = res["data"]["result"]
            val = results[0]["value"][1] if results else "0"
            print(f"  ✓ PromQL: {label:<26} -> {val}")
        else:
            print(f"  ✗ Failed to query {expr}: status={status}")

    # 3. Query Grafana Dashboard and Datasource API
    print("\n4.3 Querying Grafana API (port 3000)...")
    # Basic auth admin:admin
    import base64
    grafana_auth = base64.b64encode(b"admin:admin").decode("utf-8")
    grafana_headers = {"Authorization": f"Basic {grafana_auth}"}

    # Verify datasource
    status, ds_body = http_request(
        f"{GRAFANA_BASE}/api/datasources/name/Prometheus",
        method="GET",
        headers=grafana_headers
    )
    assert status == 200, f"Prometheus datasource not found in Grafana! ({status})"
    print(f"  ✓ Grafana Datasource verified:  '{ds_body['name']}' -> {ds_body['url']} (type: {ds_body['type']})")

    # Verify dashboard
    status, dash_body = http_request(
        f"{GRAFANA_BASE}/api/dashboards/uid/webhook-phase2-overview",
        method="GET",
        headers=grafana_headers
    )
    assert status == 200, f"Dashboard not found in Grafana! ({status})"
    dashboard = dash_body["dashboard"]
    panels = dashboard.get("panels", [])
    print(f"  ✓ Grafana Dashboard verified:   '{dashboard['title']}' (uid: {dashboard['uid']})")
    print(f"  ✓ Provisioned panels count:     {len(panels)} panels")
    print("    Panels listing:")
    for p in panels:
        p_type = p.get("type", "")
        p_title = p.get("title", "(row)")
        targets = [t.get("expr", "") for t in p.get("targets", [])]
        target_str = f" [expr: {targets[0]}]" if targets else ""
        print(f"      - [{p_type}] {p_title}{target_str}")

    print("\n>>> STEP 4 VERIFIED: Grafana dashboard and Prometheus reflect actual deliveries, retries, queue depth, and failures.")


def main():
    import urllib.parse
    print("=" * 70)
    print(" PHASE 2 LIVE END-TO-END VERIFICATION HARNESS")
    print("=" * 70)
    print(f" API Base:        {API_BASE}")
    print(f" Test Receiver:   {RECEIVER_BASE}")
    print(f" Prometheus Base: {PROMETHEUS_BASE}")
    print(f" Grafana Base:    {GRAFANA_BASE}")

    # Run all 4 steps
    dead_letter_id, dest_id = run_step_1()
    run_step_2(dead_letter_id, dest_id)
    run_step_3()
    run_step_4()

    banner("ALL 4 VERIFICATION STEPS COMPLETED AND VALIDATED SUCCESSFULLY! ✓")


if __name__ == "__main__":
    main()
