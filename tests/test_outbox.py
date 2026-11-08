"""Outbox transactional tests."""
import uuid
from datetime import datetime, timezone

from sqlalchemy import select

from app.models.event import Event
from app.models.delivery import EventDelivery
from app.models.outbox import OutboxEntry
from app.services.event_service import ingest_event, EventIngestionError
from app.services.outbox_service import claim_pending_entries, mark_published, mark_publish_error


def test_event_ingestion_creates_outbox_entries(db_session, demo_tenant, sample_destination):
    """Event ingestion must create outbox entries in the same transaction."""
    event, deliveries = ingest_event(
        session=db_session,
        tenant_id=demo_tenant.id,
        event_type="test.outbox",
        payload={"key": "value"},
        destination_ids=[sample_destination.id],
    )

    # Verify outbox entry exists
    outbox = db_session.execute(
        select(OutboxEntry).where(OutboxEntry.delivery_id == deliveries[0].id)
    ).scalar_one_or_none()

    assert outbox is not None
    assert outbox.status == "pending"
    assert outbox.published_at is None


def test_transaction_rollback_removes_all(db_session, demo_tenant):
    """If event ingestion fails, no event, delivery, or outbox records should persist."""
    fake_dest_id = uuid.uuid4()

    try:
        ingest_event(
            session=db_session,
            tenant_id=demo_tenant.id,
            event_type="test.fail",
            payload={"key": "value"},
            destination_ids=[fake_dest_id],
        )
    except EventIngestionError:
        pass

    # Verify nothing was created
    events = list(db_session.execute(
        select(Event).where(Event.tenant_id == demo_tenant.id, Event.event_type == "test.fail")
    ).scalars().all())
    assert len(events) == 0


def test_claim_pending_entries(db_session, demo_tenant, sample_destination):
    """Pending outbox entries should be returned by claim_pending_entries."""
    event, deliveries = ingest_event(
        session=db_session,
        tenant_id=demo_tenant.id,
        event_type="test.claim",
        payload={"key": "value"},
        destination_ids=[sample_destination.id],
    )

    entries = claim_pending_entries(db_session, batch_size=10)
    assert len(entries) >= 1
    assert any(e.delivery_id == deliveries[0].id for e in entries)


def test_mark_published(db_session, demo_tenant, sample_destination):
    """Marking an outbox entry as published should update status and timestamp."""
    event, deliveries = ingest_event(
        session=db_session,
        tenant_id=demo_tenant.id,
        event_type="test.publish",
        payload={"key": "value"},
        destination_ids=[sample_destination.id],
    )

    entries = claim_pending_entries(db_session, batch_size=10)
    target = next(e for e in entries if e.delivery_id == deliveries[0].id)

    mark_published(db_session, target.id)

    db_session.expire_all()
    updated = db_session.execute(
        select(OutboxEntry).where(OutboxEntry.id == target.id)
    ).scalar_one()

    assert updated.status == "published"
    assert updated.published_at is not None


def test_mark_publish_error_keeps_pending(db_session, demo_tenant, sample_destination):
    """A publish error should be recorded but the entry stays pending for retry."""
    event, deliveries = ingest_event(
        session=db_session,
        tenant_id=demo_tenant.id,
        event_type="test.error",
        payload={"key": "value"},
        destination_ids=[sample_destination.id],
    )

    entries = claim_pending_entries(db_session, batch_size=10)
    target = next(e for e in entries if e.delivery_id == deliveries[0].id)

    mark_publish_error(db_session, target.id, "Redis connection failed")

    db_session.expire_all()
    updated = db_session.execute(
        select(OutboxEntry).where(OutboxEntry.id == target.id)
    ).scalar_one()

    assert updated.status == "pending"  # Still pending
    assert updated.last_error == "Redis connection failed"


def test_one_delivery_per_destination(db_session, demo_tenant):
    """Multiple destinations should create one delivery per destination."""
    from app.models.destination import Destination

    dests = []
    for i in range(3):
        d = Destination(
            tenant_id=demo_tenant.id,
            name=f"Dest {i}",
            url=f"https://example.com/hook/{i}",
            is_active=True,
        )
        db_session.add(d)
        db_session.flush()
        dests.append(d)

    event, deliveries = ingest_event(
        session=db_session,
        tenant_id=demo_tenant.id,
        event_type="test.multi",
        payload={"key": "value"},
        destination_ids=[d.id for d in dests],
    )

    assert len(deliveries) == 3
    dest_ids = {d.destination_id for d in deliveries}
    assert dest_ids == {d.id for d in dests}

    # Verify outbox entries
    outbox_entries = list(db_session.execute(
        select(OutboxEntry).where(
            OutboxEntry.delivery_id.in_([d.id for d in deliveries])
        )
    ).scalars().all())
    assert len(outbox_entries) == 3


def test_dispatcher_batch_success(db_session, demo_tenant, sample_destination):
    """Dispatcher successfully publishes tasks and marks outbox entries as published."""
    from unittest.mock import patch
    from app.dispatcher.outbox_dispatcher import process_outbox_batch

    event, deliveries = ingest_event(
        session=db_session,
        tenant_id=demo_tenant.id,
        event_type="test.dispatch.ok",
        payload={"msg": "hello"},
        destination_ids=[sample_destination.id],
    )

    with patch("app.dispatcher.outbox_dispatcher.deliver_webhook.delay") as mock_delay:
        count = process_outbox_batch(db_session, batch_size=10)
        assert count == 1
        mock_delay.assert_called_once_with(str(deliveries[0].id))

    outbox = db_session.execute(
        select(OutboxEntry).where(OutboxEntry.delivery_id == deliveries[0].id)
    ).scalar_one()
    assert outbox.status == "published"
    assert outbox.published_at is not None


def test_dispatcher_batch_publish_failure(db_session, demo_tenant, sample_destination):
    """Dispatcher records publication failure while leaving entry pending for retry."""
    from unittest.mock import patch
    from app.dispatcher.outbox_dispatcher import process_outbox_batch

    event, deliveries = ingest_event(
        session=db_session,
        tenant_id=demo_tenant.id,
        event_type="test.dispatch.err",
        payload={"msg": "retry-me"},
        destination_ids=[sample_destination.id],
    )

    with patch("app.dispatcher.outbox_dispatcher.deliver_webhook.delay") as mock_delay:
        mock_delay.side_effect = RuntimeError("Broker connection timeout")
        count = process_outbox_batch(db_session, batch_size=10)
        assert count == 0

    outbox = db_session.execute(
        select(OutboxEntry).where(OutboxEntry.delivery_id == deliveries[0].id)
    ).scalar_one()
    assert outbox.status == "pending"
    assert "Broker connection timeout" in (outbox.last_error or "")

