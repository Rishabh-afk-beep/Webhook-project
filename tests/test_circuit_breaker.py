"""Tests for distributed circuit breaker."""
import time
from unittest.mock import MagicMock, patch
import pytest

from app.core.circuit_breaker import CircuitBreaker, CircuitState, get_circuit_breaker
from app.models.destination import Destination
from app.models.delivery import EventDelivery
from app.models.event import Event
from app.workers.tasks import deliver_webhook


class DummyCircuitRedis:
    """In-memory Redis mock supporting circuit breaker operations."""

    def __init__(self):
        self.strings = {}
        self.zsets = {}

    def get(self, key):
        val = self.strings.get(key)
        if val is None:
            return None
        if isinstance(val, str):
            return val.encode("utf-8")
        return str(val).encode("utf-8")

    def set(self, key, value):
        self.strings[key] = value

    def delete(self, key):
        self.strings.pop(key, None)
        self.zsets.pop(key, None)

    def incr(self, key):
        current = int(self.strings.get(key, 0)) + 1
        self.strings[key] = current
        return current

    def pipeline(self):
        return DummyPipeline(self)


class DummyPipeline:
    def __init__(self, parent: DummyCircuitRedis):
        self.parent = parent
        self.commands = []

    def set(self, key, val):
        self.commands.append(("set", key, val))
        return self

    def delete(self, key):
        self.commands.append(("delete", key))
        return self

    def zremrangebyscore(self, key, min_s, max_s):
        self.commands.append(("zremrangebyscore", key, min_s, max_s))
        return self

    def zadd(self, key, mapping):
        self.commands.append(("zadd", key, mapping))
        return self

    def zcard(self, key):
        self.commands.append(("zcard", key))
        return self

    def expire(self, key, ttl):
        self.commands.append(("expire", key, ttl))
        return self

    def execute(self):
        results = []
        for cmd in self.commands:
            op = cmd[0]
            if op == "set":
                self.parent.strings[cmd[1]] = cmd[2]
                results.append(True)
            elif op == "delete":
                self.parent.delete(cmd[1])
                results.append(1)
            elif op == "zremrangebyscore":
                z = self.parent.zsets.setdefault(cmd[1], {})
                to_del = [k for k, v in z.items() if cmd[2] <= v <= cmd[3]]
                for k in to_del:
                    del z[k]
                results.append(len(to_del))
            elif op == "zadd":
                z = self.parent.zsets.setdefault(cmd[1], {})
                z.update(cmd[2])
                results.append(len(cmd[2]))
            elif op == "zcard":
                z = self.parent.zsets.setdefault(cmd[1], {})
                results.append(len(z))
            elif op == "expire":
                results.append(True)
        return results


class TestCircuitBreakerUnit:
    """Unit tests for CircuitBreaker state transitions."""

    def test_circuit_starts_closed(self):
        """A new destination starts in CLOSED state and allows requests."""
        redis_mock = DummyCircuitRedis()
        cb = CircuitBreaker(redis_client=redis_mock)

        allowed, state, delay = cb.can_execute("dest-new")
        assert allowed is True
        assert state == CircuitState.CLOSED
        assert delay == 0

    def test_repeated_failures_open_circuit(self):
        """Reaching failure threshold opens the circuit."""
        redis_mock = DummyCircuitRedis()
        cb = CircuitBreaker(redis_client=redis_mock)
        dest_id = "dest-failing"

        # Record 5 failures (default threshold = 5)
        for _ in range(5):
            cb.record_failure(dest_id, is_qualifying_failure=True)

        assert cb.get_state(dest_id) == CircuitState.OPEN

        # Next check should be blocked
        allowed, state, delay = cb.can_execute(dest_id)
        assert allowed is False
        assert state == CircuitState.OPEN
        assert delay > 0

    def test_non_qualifying_failure_does_not_open_circuit(self):
        """4xx errors (non-qualifying) do not count towards opening the circuit."""
        redis_mock = DummyCircuitRedis()
        cb = CircuitBreaker(redis_client=redis_mock)
        dest_id = "dest-4xx"

        for _ in range(10):
            cb.record_failure(dest_id, is_qualifying_failure=False)

        assert cb.get_state(dest_id) == CircuitState.CLOSED
        allowed, state, _ = cb.can_execute(dest_id)
        assert allowed is True
        assert state == CircuitState.CLOSED

    def test_circuit_transitions_to_half_open_after_open_duration(self):
        """After open_duration_seconds has passed, state transitions to HALF_OPEN."""
        redis_mock = DummyCircuitRedis()
        cb = CircuitBreaker(redis_client=redis_mock)
        dest_id = "dest-half-open"

        # Open the circuit
        for _ in range(5):
            cb.record_failure(dest_id, is_qualifying_failure=True)
        assert cb.get_state(dest_id) == CircuitState.OPEN

        # Advance time by 61 seconds (default open duration = 60s)
        now = time.time()
        with patch("time.time", return_value=now + 65):
            allowed, state, delay = cb.can_execute(dest_id)
            assert allowed is True
            assert state == CircuitState.HALF_OPEN
            assert delay == 0

    def test_successful_probes_close_circuit(self):
        """Meeting success threshold in HALF_OPEN restores circuit to CLOSED."""
        redis_mock = DummyCircuitRedis()
        cb = CircuitBreaker(redis_client=redis_mock)
        dest_id = "dest-recovery"

        # Set to HALF_OPEN
        redis_mock.strings[f"circuit_breaker:dest:{dest_id}:state"] = CircuitState.HALF_OPEN.value

        # Record 2 consecutive successes (default success_threshold = 2)
        cb.record_success(dest_id)
        assert cb.get_state(dest_id) == CircuitState.HALF_OPEN

        cb.record_success(dest_id)
        assert cb.get_state(dest_id) == CircuitState.CLOSED

    def test_failed_probe_reopens_circuit(self):
        """A failure during HALF_OPEN immediately reopens the circuit."""
        redis_mock = DummyCircuitRedis()
        cb = CircuitBreaker(redis_client=redis_mock)
        dest_id = "dest-probe-fail"

        redis_mock.strings[f"circuit_breaker:dest:{dest_id}:state"] = CircuitState.HALF_OPEN.value

        cb.record_failure(dest_id, is_qualifying_failure=True)
        assert cb.get_state(dest_id) == CircuitState.OPEN

    def test_unrelated_destinations_independent(self):
        """Failures on one destination do not affect others."""
        redis_mock = DummyCircuitRedis()
        cb = CircuitBreaker(redis_client=redis_mock)

        # Open dest-X
        for _ in range(5):
            cb.record_failure("dest-X", is_qualifying_failure=True)

        assert cb.get_state("dest-X") == CircuitState.OPEN
        assert cb.get_state("dest-Y") == CircuitState.CLOSED

        allowed_y, state_y, _ = cb.can_execute("dest-Y")
        assert allowed_y is True
        assert state_y == CircuitState.CLOSED


class TestCircuitBreakerWorkerIntegration:
    """Integration test verifying worker behavior when destination circuit is OPEN."""

    @patch("app.workers.tasks.create_session")
    @patch("app.workers.tasks.get_circuit_breaker")
    def test_circuit_open_defers_delivery_without_fake_attempt(
        self, mock_get_cb, mock_create_session, db_session, demo_tenant
    ):
        """When destination circuit is OPEN, delivery is deferred without an HTTP attempt."""
        dest = Destination(tenant_id=demo_tenant.id, name="Broken Dest", url="http://down.com/hook")
        db_session.add(dest)
        db_session.flush()

        event = Event(tenant_id=demo_tenant.id, event_type="order.failed", payload={})
        db_session.add(event)
        db_session.flush()

        delivery = EventDelivery(
            tenant_id=demo_tenant.id,
            event_id=event.id,
            destination_id=dest.id,
            status="pending",
        )
        db_session.add(delivery)
        db_session.commit()

        mock_create_session.return_value = db_session

        mock_cb = MagicMock()
        mock_cb.can_execute.return_value = (False, CircuitState.OPEN, 30)
        mock_get_cb.return_value = mock_cb

        with patch("app.workers.tasks.deliver_webhook.apply_async") as mock_apply:
            res = deliver_webhook(str(delivery.id))
            assert res["status"] == "retry_scheduled"
            assert res["reason"] == "circuit_open"
            assert res["next_attempt_in_seconds"] == 30
            mock_apply.assert_called_once()

        db_session.expire_all()
        updated = db_session.get(EventDelivery, delivery.id)
        assert updated.status == "retry_scheduled"
        assert updated.attempt_count == 0
        assert len(updated.attempts) == 0

