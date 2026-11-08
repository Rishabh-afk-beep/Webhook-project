"""Automated Staged Load Testing Harness (5 to 500 Virtual Users).

Executes sequential load testing stages:
  1. Baseline (5 VUs)
  2. Moderate (20 VUs)
  3. High (50 VUs)
  4. Stress (100 VUs)
  5. Extended Stress (250 VUs)
  6. Target Stress (500 VUs)

Measures:
  - API Ingestion throughput (req/s) and latency percentiles (p50, p90, p95, p99)
  - Asynchronous delivery drain time, completion rate, and throughput
  - Queue depth and drain verification in PostgreSQL and Redis
  - Webhook delivery verification at test receiver
  - Container CPU and memory utilization (API, Worker, Dispatcher, Postgres, Redis)
  - Identifies performance bottlenecks across stages

Outputs results to reports/load_test_results.json.
"""
import os
import sys
import json
import time
import subprocess
import urllib.request
import urllib.parse
from datetime import datetime, timezone

API_KEY = os.environ.get("API_KEY", "j6ZHYRMBwKcElXDcdffBMahJPmD2WUK2ifcIhGhdiY4")
NETWORK = "reliablewebhook_default"
API_URL = "http://api:8000"
RECEIVER_URL = "http://localhost:9000"
REPORT_PATH = os.path.join("reports", "load_test_results.json")

STAGES = [
    {"name": "Stage 1: Baseline", "vus": 5, "duration": "10s", "slo_p95_ms": 100},
    {"name": "Stage 2: Moderate", "vus": 20, "duration": "15s", "slo_p95_ms": 250},
    {"name": "Stage 3: High", "vus": 50, "duration": "15s", "slo_p95_ms": 500},
    {"name": "Stage 4: Stress", "vus": 100, "duration": "15s", "slo_p95_ms": 1000},
    {"name": "Stage 5: Extended Stress", "vus": 250, "duration": "15s", "slo_p95_ms": 2500},
    {"name": "Stage 6: Target Stress", "vus": 500, "duration": "15s", "slo_p95_ms": 3000},
]


def run_cmd(cmd: list[str]) -> str:
    """Run a command and return stdout."""
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
    return res.stdout.strip()


def clear_test_receiver():
    """Clear test receiver buffer before each stage and ensure high destination rate limit."""
    cmd_clear = [
        "docker", "compose", "exec", "-T", "test-receiver",
        "python", "-c",
        "import urllib.request; req = urllib.request.Request('http://localhost:9000/requests', method='DELETE'); urllib.request.urlopen(req)"
    ]
    run_cmd(cmd_clear)

    cmd_ratelimit = [
        "docker", "compose", "exec", "-T", "postgres",
        "psql", "-U", "webhook_user", "-d", "webhook_db", "-c",
        "UPDATE destinations SET rate_limit_per_second = 2000.0, rate_limit_burst = 5000 WHERE url LIKE '%/webhooks/success';"
    ]
    run_cmd(cmd_ratelimit)


def get_receiver_stats() -> dict:
    """Query test receiver total count and delivery audit."""
    cmd = [
        "docker", "compose", "exec", "-T", "test-receiver",
        "python", "-c",
        "import urllib.request; print(urllib.request.urlopen('http://localhost:9000/delivery-audit').read().decode())"
    ]
    raw = run_cmd(cmd)
    try:
        data = json.loads(raw)
        return {
            "total_count": data.get("total_received_count", 0),
            "unique_delivery_ids": data.get("unique_delivery_ids_count", 0),
            "duplicate_requests": data.get("duplicate_requests_count", 0),
        }
    except Exception as e:
        print(f"Warning: get_receiver_stats parsing failed: raw={raw!r}, error={e}")
        return {"total_count": 0, "unique_delivery_ids": 0, "duplicate_requests": 0}


def get_db_stats() -> dict:
    """Query PostgreSQL pending outbox and delivery counts."""
    sql = "SELECT (SELECT count(*) FROM outbox_entries WHERE status = 'pending'), (SELECT count(*) FROM event_deliveries WHERE status IN ('pending', 'processing')), (SELECT count(*) FROM event_deliveries WHERE status = 'succeeded'), (SELECT count(*) FROM event_deliveries WHERE status = 'failed'), (SELECT count(*) FROM event_deliveries WHERE status = 'dead_lettered');"
    cmd = [
        "docker", "compose", "exec", "-T", "postgres",
        "psql", "-U", "webhook_user", "-d", "webhook_db", "-t", "-A", "-c", sql
    ]
    raw = run_cmd(cmd)
    try:
        parts = raw.split("|")
        return {
            "pending_outbox": int(parts[0]),
            "pending_deliveries": int(parts[1]),
            "succeeded_deliveries": int(parts[2]),
            "failed_deliveries": int(parts[3]),
            "dead_lettered_deliveries": int(parts[4]),
        }
    except Exception as e:
        print(f"Warning: get_db_stats parsing failed: raw={raw!r}, error={e}")
        return {
            "pending_outbox": -1,
            "pending_deliveries": -1,
            "succeeded_deliveries": -1,
            "failed_deliveries": -1,
            "dead_lettered_deliveries": -1,
        }


def get_redis_queue_depth() -> int:
    """Query Redis celery queue length."""
    cmd = ["docker", "compose", "exec", "-T", "redis", "redis-cli", "llen", "celery"]
    out = run_cmd(cmd)
    try:
        return int(out)
    except Exception:
        return 0


def get_container_stats() -> dict:
    """Sample container CPU and Memory metrics."""
    cmd = [
        "docker", "stats", "--no-stream",
        "--format", "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}"
    ]
    lines = run_cmd(cmd).splitlines()
    stats = {}
    for line in lines:
        parts = line.split("|")
        if len(parts) >= 3:
            name = parts[0]
            cpu = parts[1].replace("%", "").strip()
            mem = parts[2].split("/")[0].strip()
            for key in ["api", "worker", "outbox-dispatcher", "postgres", "redis"]:
                if key in name:
                    stats[key] = {
                        "cpu_percent": float(cpu) if cpu.replace(".", "", 1).isdigit() else 0.0,
                        "mem_usage": mem,
                    }
    return stats


def wait_for_queue_drain(timeout_seconds: int = 45) -> tuple[bool, float, dict]:
    """Poll DB and Redis until pending queues reach 0. Return (drained, elapsed_seconds, final_stats)."""
    start = time.time()
    while time.time() - start < timeout_seconds:
        db = get_db_stats()
        redis_depth = get_redis_queue_depth()
        if (
            db.get("pending_outbox", 1) == 0
            and db.get("pending_deliveries", 1) == 0
            and redis_depth == 0
        ):
            return True, time.time() - start, db
        time.sleep(0.5)
    return False, time.time() - start, get_db_stats()


def run_stage(stage: dict) -> dict:
    """Execute a single staged load test with k6 and capture full telemetry."""
    vus = stage["vus"]
    duration = stage["duration"]
    name = stage["name"]
    print(f"\n{'='*70}")
    print(f" >>> RUNNING {name.upper()}: {vus} VUs for {duration}")
    print(f"{'='*70}")

    clear_test_receiver()
    initial_db = get_db_stats()
    initial_redis = get_redis_queue_depth()
    initial_resources = get_container_stats()

    summary_file = f"/work/reports/k6_stage_{vus}vu.json"
    host_summary_file = os.path.join("reports", f"k6_stage_{vus}vu.json")
    if os.path.exists(host_summary_file):
        try:
            os.remove(host_summary_file)
        except Exception:
            pass

    k6_cmd = [
        "docker", "run", "--rm",
        "--network", NETWORK,
        "-v", f"{os.getcwd()}:/work",
        "-w", "/work",
        "grafana/k6", "run",
        "--no-thresholds",
        "-u", str(vus),
        "-d", duration,
        "-e", f"API_BASE_URL={API_URL}",
        "-e", f"API_KEY={API_KEY}",
        "--summary-trend-stats", "avg,min,med,max,p(90),p(95),p(99)",
        "--summary-export", summary_file,
        "load_tests/k6/scenarios/event_ingestion.js",
    ]

    print(f"Launching k6 load test ({vus} VUs)...")
    ingestion_start = time.time()
    k6_proc = subprocess.run(k6_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
    ingestion_time = time.time() - ingestion_start
    print(f"k6 ingestion completed in {ingestion_time:.2f}s")

    # Sample resource utilization during peak
    peak_resources = get_container_stats()

    # Wait for asynchronous delivery to drain
    print("Monitoring asynchronous delivery queue drain...")
    drain_start = time.time()
    drain_timeout = max(60, int(vus * 0.4) + 30)
    drained, drain_time, final_db = wait_for_queue_drain(timeout_seconds=drain_timeout)
    print(f"Queue drain status: {'COMPLETED' if drained else 'TIMED OUT'} in {drain_time:.2f}s")

    # Query test receiver received count
    receiver_stats = get_receiver_stats()
    receiver_delivered = receiver_stats.get("total_count", 0)

    # Parse k6 summary JSON
    k6_metrics = {}
    if os.path.exists(host_summary_file):
        try:
            with open(host_summary_file, "r", encoding="utf-8") as f:
                k6_raw = json.load(f)
            metrics = k6_raw.get("metrics", {})
            http_reqs = (
                metrics.get("http_reqs", {}).get("count")
                or metrics.get("http_reqs", {}).get("values", {}).get("count", 0)
            )
            http_req_rate = (
                metrics.get("http_reqs", {}).get("rate")
                or metrics.get("http_reqs", {}).get("values", {}).get("rate", 0)
            )
            failed_obj = metrics.get("http_req_failed", {})
            http_failed_rate = (
                failed_obj.get("value")
                if "value" in failed_obj
                else failed_obj.get("values", {}).get("rate", 0)
            )
            duration_vals = (
                metrics.get("http_req_duration", {}).get("values")
                or metrics.get("http_req_duration", {})
            )

            k6_metrics = {
                "total_requests": int(http_reqs),
                "throughput_rps": round(http_req_rate, 2),
                "failed_rate": round(http_failed_rate, 4),
                "p50_ms": round(duration_vals.get("med", 0), 2),
                "p90_ms": round(duration_vals.get("p(90)", 0), 2),
                "p95_ms": round(duration_vals.get("p(95)", 0), 2),
                "p99_ms": round(duration_vals.get("p(99)", 0), 2),
                "min_ms": round(duration_vals.get("min", 0), 2),
                "max_ms": round(duration_vals.get("max", 0), 2),
                "avg_ms": round(duration_vals.get("avg", 0), 2),
            }
        except Exception as e:
            print(f"Warning: Failed to parse k6 summary JSON: {e}")

    # Calculate asynchronous delivery metrics
    events_ingested = k6_metrics.get("total_requests", 0) - 1  # Subtract setup() call
    if events_ingested < 0:
        events_ingested = 0

    delta_succeeded = (
        final_db.get("succeeded_deliveries", 0) - initial_db.get("succeeded_deliveries", 0)
    )
    receiver_delivered = receiver_stats.get("total_count", 0)
    unique_delivered = receiver_stats.get("unique_delivery_ids", 0)
    duplicate_delivered = receiver_stats.get("duplicate_requests", 0)
    actual_delivered = max(receiver_delivered, delta_succeeded)

    total_test_time = ingestion_time + drain_time
    delivery_throughput = (
        round(actual_delivered / total_test_time, 2)
        if total_test_time > 0
        else 0
    )

    slo_p95 = stage.get("slo_p95_ms", 1000)
    p95_actual = k6_metrics.get("p95_ms", 0)
    error_rate = k6_metrics.get("failed_rate", 1.0)

    # Classify stage with explicit criteria:
    if drained and error_rate < 0.01 and p95_actual <= slo_p95:
        stage_status = "PASSED_WITHIN_SLO"
        passed = True
    elif drained and error_rate < 0.01 and p95_actual > slo_p95:
        stage_status = "SATURATED_DEGRADED"
        passed = True  # Proceed to test saturation limits
    else:
        stage_status = "FAILED"
        passed = False

    stage_result = {
        "stage": name,
        "vus": vus,
        "configured_duration": duration,
        "actual_ingestion_time_seconds": round(ingestion_time, 2),
        "ingestion_metrics": k6_metrics,
        "slo_evaluation": {
            "slo_p95_ms_threshold": slo_p95,
            "p95_actual_ms": p95_actual,
            "latency_slo_met": p95_actual <= slo_p95,
            "error_rate_threshold": 0.01,
            "error_rate_actual": error_rate,
            "error_slo_met": error_rate < 0.01,
            "queue_drained": drained,
            "stage_status": stage_status,
        },
        "asynchronous_delivery": {
            "events_ingested": events_ingested,
            "db_succeeded_deliveries_delta": delta_succeeded,
            "webhooks_received_at_destination": receiver_delivered,
            "unique_delivery_ids_received": unique_delivered,
            "duplicate_requests_received": duplicate_delivered,
            "missing_deliveries": max(0, events_ingested - unique_delivered),
            "delivery_completion_rate": (
                round(unique_delivered / events_ingested, 4) if events_ingested > 0 else 1.0
            ),
            "queue_drain_time_seconds": round(drain_time, 2),
            "queue_fully_drained": drained,
            "delivery_throughput_dps": delivery_throughput,
            "initial_db_state": initial_db,
            "final_db_state": final_db,
        },
        "resource_utilization": {
            "initial": initial_resources,
            "peak": peak_resources,
        },
        "passed": passed,
        "status": stage_status,
    }

    print(f"--- {name} Results ---")
    print(f"  Ingestion Throughput: {k6_metrics.get('throughput_rps')} req/s | Total Requests: {k6_metrics.get('total_requests')}")
    print(f"  Latency (ms): p50={k6_metrics.get('p50_ms')} | p90={k6_metrics.get('p90_ms')} | p95={k6_metrics.get('p95_ms')} (SLO: <={slo_p95}ms) | p99={k6_metrics.get('p99_ms')}")
    print(f"  Deliveries: Unique={unique_delivered} / {events_ingested} | Duplicates={duplicate_delivered} | Missing={max(0, events_ingested - unique_delivered)}")
    print(f"  Queue Drain Time: {drain_time:.2f}s | Queue Fully Drained: {drained}")
    print(f"  Delivery Throughput: {delivery_throughput} deliveries/sec")
    print(f"  Stage Evaluation: [{stage_status}]")

    return stage_result


def main():
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass
    os.makedirs("reports", exist_ok=True)
    all_stages_results = []
    print("\n" + "#"*70)
    print(" STARTING STAGED LOAD BENCHMARK (5 -> 500 VIRTUAL USERS)")
    print("#"*70)

    for stage in STAGES:
        res = run_stage(stage)
        all_stages_results.append(res)
        if not res["passed"]:
            print(f"\n[ALERT] Stage '{stage['name']}' did not meet pass criteria. Halting further escalation.")
            break
        # Grace period between stages
        time.sleep(3)

    final_report = {
        "benchmark_suite": "Staged Load Benchmark (5 - 500 VUs)",
        "executed_at": datetime.now(timezone.utc).isoformat(),
        "total_stages_run": len(all_stages_results),
        "stages_passed": sum(1 for s in all_stages_results if s["passed"]),
        "stages": all_stages_results,
    }

    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(final_report, f, indent=2)

    print("\n" + "="*70)
    print(" STAGED LOAD BENCHMARK SUMMARY TABLE")
    print("="*70)
    print(f"{'Stage':<24} | {'VUs':<5} | {'Req/s':<8} | {'p50(ms)':<8} | {'p95(ms)':<8} | {'Unique Del':<10} | {'Dups':<5} | {'Drain(s)':<8} | {'Status':<18}")
    print("-" * 115)
    for s in all_stages_results:
        ing = s["ingestion_metrics"]
        ad = s["asynchronous_delivery"]
        print(f"{s['stage']:<24} | {s['vus']:<5} | {ing.get('throughput_rps', 0):<8} | {ing.get('p50_ms', 0):<8} | {ing.get('p95_ms', 0):<8} | {ad.get('unique_delivery_ids_received', 0):<10} | {ad.get('duplicate_requests_received', 0):<5} | {ad.get('queue_drain_time_seconds', 0):<8} | {s.get('status', 'PASSED'):<18}")

    print(f"\nFull raw benchmark output saved to: {REPORT_PATH}")


if __name__ == "__main__":
    main()

