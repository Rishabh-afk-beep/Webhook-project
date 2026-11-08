"""End-to-end smoke test script for Reliable Webhook Delivery Platform.

Validates the full delivery pipeline:
1. Health and readiness checks
2. Destination creation (success and failure targets)
3. Event ingestion with transactional outbox
4. Async worker execution and status updates
5. Delivery attempts audit log verification
6. Authentication & error handling

Usage:
    python -m scripts.smoke_test [--api-url http://localhost:8000] [--api-key <KEY>]
"""
import argparse
import os
import sys
import time
import httpx

# Ensure proper encoding for console output on Windows
if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass


def run_smoke_test(api_url: str, api_key: str, receiver_url: str) -> bool:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    client = httpx.Client(timeout=10.0)

    print("=" * 65)
    print(" Reliable Webhook Delivery Platform — End-to-End Smoke Test")
    print("=" * 65)
    print(f" API URL:          {api_url}")
    print(f" Receiver URL:     {receiver_url}")
    print(f" API Key Prefix:   {api_key[:12]}...")
    print("-" * 65)

    all_passed = True

    # 1. Health checks
    print("\n[1/6] Testing Health & Readiness Endpoints...")
    try:
        r = client.get(f"{api_url}/health")
        assert r.status_code == 200, f"Expected 200, got {r.status_code}"
        assert r.json().get("status") == "ok"
        print("  ✓ GET /health -> 200 OK")

        r = client.get(f"{api_url}/health/ready")
        assert r.status_code == 200, f"Expected 200, got {r.status_code}"
        assert r.json().get("status") == "ready"
        print("  ✓ GET /health/ready -> 200 OK (Database reachable)")
    except Exception as e:
        print(f"  ✗ Health check failed: {e}")
        return False

    # 2. Authentication check
    print("\n[2/6] Testing Authentication Enforcement...")
    try:
        r = client.get(f"{api_url}/v1/destinations", headers={"Authorization": "Bearer invalid_key"})
        assert r.status_code == 401, f"Expected 401 for invalid key, got {r.status_code}"
        print("  ✓ Invalid API key rejected with 401 Unauthorized")
    except Exception as e:
        print(f"  ✗ Auth check failed: {e}")
        all_passed = False

    # 3. Destination creation
    print("\n[3/6] Registering Webhook Destinations...")
    success_dest_id = None
    failure_dest_id = None
    try:
        success_dest_url = f"{receiver_url}/webhooks/success"
        r = client.post(
            f"{api_url}/v1/destinations",
            headers=headers,
            json={"name": "Smoke Test Receiver Success", "url": success_dest_url},
        )
        assert r.status_code == 201, f"Expected 201, got {r.status_code}: {r.text}"
        success_dest_id = r.json()["id"]
        print(f"  ✓ Created success destination: {success_dest_id} ({success_dest_url})")

        fail_dest_url = f"{receiver_url}/webhooks/failure"
        r = client.post(
            f"{api_url}/v1/destinations",
            headers=headers,
            json={"name": "Smoke Test Receiver Failure", "url": fail_dest_url},
        )
        assert r.status_code == 201, f"Expected 201, got {r.status_code}: {r.text}"
        failure_dest_id = r.json()["id"]
        print(f"  ✓ Created failure destination: {failure_dest_id} ({fail_dest_url})")
    except Exception as e:
        print(f"  ✗ Destination registration failed: {e}")
        return False

    # 4. Ingest event to success destination
    print("\n[4/6] Ingesting Event (Success Scenario)...")
    try:
        payload = {
            "event_type": "smoke.test.order_placed",
            "payload": {"order_id": "ord_smoke_999", "amount": 4999, "currency": "INR"},
            "destination_ids": [success_dest_id],
        }
        r = client.post(f"{api_url}/v1/events", headers=headers, json=payload)
        assert r.status_code == 202, f"Expected 202, got {r.status_code}: {r.text}"
        resp_data = r.json()
        event_id = resp_data["event_id"]
        delivery_id = resp_data["deliveries"][0]["delivery_id"]
        print(f"  ✓ Event accepted (202): Event ID {event_id}")
        print(f"  ✓ Delivery created:    {delivery_id}")

        # Poll for completion
        print("  ... Polling delivery status for background execution ...")
        delivery_status = None
        for i in range(20):
            time.sleep(1)
            dr = client.get(f"{api_url}/v1/deliveries/{delivery_id}", headers=headers)
            if dr.status_code == 200:
                data = dr.json()
                delivery_status = data.get("status")
                if delivery_status in ("succeeded", "failed"):
                    break

        print(f"  ✓ Final Delivery Status: {delivery_status}")
        assert delivery_status == "succeeded", f"Delivery did not succeed: {delivery_status}"

        # Check attempts
        ar = client.get(f"{api_url}/v1/deliveries/{delivery_id}/attempts", headers=headers)
        assert ar.status_code == 200
        ar_data = ar.json()
        attempts = ar_data.get("items", ar_data.get("attempts", []))
        assert len(attempts) >= 1, "No delivery attempts recorded"
        print(f"  ✓ Delivery attempt recorded: HTTP {attempts[0]['http_status']} ({attempts[0]['response_time_ms']}ms)")
    except Exception as e:
        print(f"  ✗ Success delivery flow failed: {e}")
        all_passed = False

    # 5. Ingest event to failure destination
    print("\n[5/6] Ingesting Event (Failure Scenario)...")
    try:
        payload = {
            "event_type": "smoke.test.failure_expected",
            "payload": {"reason": "testing 500 handling"},
            "destination_ids": [failure_dest_id],
        }
        r = client.post(f"{api_url}/v1/events", headers=headers, json=payload)
        assert r.status_code == 202, f"Expected 202, got {r.status_code}: {r.text}"
        resp_data = r.json()
        delivery_id = resp_data["deliveries"][0]["delivery_id"]
        print(f"  ✓ Event accepted (202): Delivery ID {delivery_id}")

        print("  ... Polling delivery status for retry/failure recording ...")
        delivery_status = None
        for i in range(20):
            time.sleep(1)
            dr = client.get(f"{api_url}/v1/deliveries/{delivery_id}", headers=headers)
            if dr.status_code == 200:
                data = dr.json()
                delivery_status = data.get("status")
                if delivery_status in ("retry_scheduled", "dead_lettered", "failed"):
                    break

        print(f"  ✓ Delivery Status under failure: {delivery_status}")
        assert delivery_status in ("retry_scheduled", "dead_lettered", "failed"), f"Unexpected delivery status: {delivery_status}"

        # Check attempts
        ar = client.get(f"{api_url}/v1/deliveries/{delivery_id}/attempts", headers=headers)
        assert ar.status_code == 200
        ar_data = ar.json()
        attempts = ar_data.get("items", ar_data.get("attempts", []))
        assert len(attempts) >= 1, "No delivery attempts recorded for failure"
        print(f"  ✓ Failure attempt audit recorded: outcome='{attempts[0]['outcome']}', HTTP {attempts[0]['http_status']}, category='{attempts[0].get('error_category')}'")
    except Exception as e:
        print(f"  ✗ Failure delivery flow failed: {e}")
        all_passed = False

    # 6. Prometheus Metrics Check
    print("\n[6/7] Testing Prometheus Metrics Endpoint...")
    try:
        mr = client.get(f"{api_url}/metrics")
        assert mr.status_code == 200, f"Expected 200 from /metrics, got {mr.status_code}"
        assert "webhook_events_received_total" in mr.text
        assert "webhook_delivery_attempts_total" in mr.text
        print("  ✓ GET /metrics -> 200 OK (Prometheus metrics active)")
    except Exception as e:
        print(f"  ✗ Metrics check failed: {e}")
        all_passed = False

    # 7. Dead-Letter Queue API Check
    print("\n[7/7] Testing Dead-Letter Queue (DLQ) API...")
    try:
        dlq_r = client.get(f"{api_url}/v1/dead-letters", headers=headers)
        assert dlq_r.status_code == 200, f"Expected 200 from /v1/dead-letters, got {dlq_r.status_code}"
        dlq_data = dlq_r.json()
        assert "items" in dlq_data
        assert "total" in dlq_data
        print(f"  ✓ GET /v1/dead-letters -> 200 OK (DLQ items: {dlq_data['total']})")
    except Exception as e:
        print(f"  ✗ DLQ check failed: {e}")
        all_passed = False

    # Summary
    print("\n" + "=" * 65)
    if all_passed:
        print("  ALL 7 SMOKE TESTS PASSED SUCCESSFULLY! ✓")
    else:
        print("  SOME SMOKE TESTS FAILED. ✗")
    print("=" * 65 + "\n")
    return all_passed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reliable Webhook Delivery Platform Smoke Test")
    parser.add_argument("--api-url", default=os.environ.get("API_BASE_URL", "http://localhost:8000"))
    parser.add_argument("--api-key", default=os.environ.get("DEMO_API_KEY", ""))
    parser.add_argument("--receiver-url", default=os.environ.get("TEST_RECEIVER_URL", "http://test-receiver:9000"))
    args = parser.parse_args()

    if not args.api_key:
        print("ERROR: --api-key is required (or set DEMO_API_KEY environment variable)")
        print("Run `python -m scripts.create_demo_tenant` first to generate an API key.")
        sys.exit(1)

    success = run_smoke_test(args.api_url, args.api_key, args.receiver_url)
    sys.exit(0 if success else 1)
