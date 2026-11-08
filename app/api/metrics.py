"""Prometheus metrics endpoint."""
from fastapi import APIRouter, Response
from sqlalchemy import select, func

from app.core.metrics import (
    get_metrics_content,
    OUTBOX_PENDING_GAUGE,
    QUEUE_DEPTH_GAUGE,
    DELIVERIES_TOTAL,
    DELIVERY_ATTEMPTS_TOTAL,
    DELIVERY_RETRIES_TOTAL,
    DEAD_LETTERS_TOTAL,
)
from app.db.session import create_session
from app.models.outbox import OutboxEntry
from app.models.delivery import EventDelivery
from app.models.delivery_attempt import DeliveryAttempt
from app.core.redis_client import get_redis_client

router = APIRouter(tags=["monitoring"])

_synced_attempt_ids: set[str] = set()


def _sync_counter(counter, new_total: int | float):
    """Safely increment a Prometheus counter to match an authoritative total."""
    try:
        current = counter._value.get()
        diff = new_total - current
        if diff > 0:
            counter.inc(diff)
    except Exception:
        pass


@router.get("/metrics")
def metrics_endpoint():
    """Expose Prometheus metrics for scraping."""
    # Synchronize operational metrics from authoritative DB and Redis
    try:
        session = create_session()
        try:
            # 1. Outbox pending gauge
            pending_outbox = session.execute(
                select(func.count(OutboxEntry.id)).where(OutboxEntry.status == "pending")
            ).scalar_one()
            OUTBOX_PENDING_GAUGE.set(pending_outbox)

            # 2. Total attempts by outcome
            att_stmt = (
                select(DeliveryAttempt.outcome, func.count(DeliveryAttempt.id))
                .group_by(DeliveryAttempt.outcome)
            )
            for outcome, count in session.execute(att_stmt).all():
                _sync_counter(DELIVERY_ATTEMPTS_TOTAL.labels(outcome=outcome, error_category="all"), count)

            # 3. Deliveries by status
            del_stmt = (
                select(EventDelivery.status, func.count(EventDelivery.id))
                .group_by(EventDelivery.status)
            )
            for status, count in session.execute(del_stmt).all():
                _sync_counter(DELIVERIES_TOTAL.labels(status=status), count)
                if status == "dead_lettered":
                    _sync_counter(DEAD_LETTERS_TOTAL, count)

            # 4. Retries total
            retries_stmt = select(
                func.coalesce(func.sum(EventDelivery.attempt_count - 1), 0)
            ).where(EventDelivery.attempt_count > 1)
            total_retries = session.execute(retries_stmt).scalar_one()
            _sync_counter(DELIVERY_RETRIES_TOTAL, total_retries)

            # 5. Delivery duration histogram sync
            recent_attempts = session.execute(
                select(DeliveryAttempt.id, DeliveryAttempt.response_time_ms)
                .where(DeliveryAttempt.response_time_ms.is_not(None))
                .order_by(DeliveryAttempt.started_at.desc())
                .limit(200)
            ).all()
            for att_id, r_ms in recent_attempts:
                str_id = str(att_id)
                if str_id not in _synced_attempt_ids:
                    _synced_attempt_ids.add(str_id)
                    from app.core.metrics import DELIVERY_DURATION_SECONDS
                    DELIVERY_DURATION_SECONDS.observe((r_ms or 0) / 1000.0)
            if len(_synced_attempt_ids) > 5000:
                _synced_attempt_ids.clear()

        finally:
            session.close()
    except Exception:
        pass

    try:
        r = get_redis_client()
        q_len = r.llen("celery")
        QUEUE_DEPTH_GAUGE.set(q_len)
    except Exception:
        pass

    content, content_type = get_metrics_content()
    return Response(content=content, media_type=content_type)
