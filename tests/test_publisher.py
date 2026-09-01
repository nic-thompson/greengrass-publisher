import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from event_schema_contracts.base.identity import derive_device_id
from event_schema_contracts.base.metadata import EventMetadata
from event_schema_contracts.base.trace import PipelineStage, TraceContext
from event_schema_contracts.telemetry.sip_registration_event import (
    RegistrationStatus,
    SipRegistrationEvent,
    SipRegistrationPayload,
)

from greengrass_publisher.publisher import (
    GreengrassEventPublisher,
    PublishError,
    StoreMismatchError,
)

STORE_ID = "store-0042"


class FakeIPCClient:
    """Records every publish call instead of talking to real IoT Core."""

    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.published: list[dict] = []

    def publish_to_iot_core(self, topic: str, payload: bytes, qos: int) -> None:
        if self.fail:
            raise RuntimeError("simulated IPC failure")
        self.published.append({"topic": topic, "payload": payload, "qos": qos})


def make_event(
    store_id: str = STORE_ID,
    device_label: str = "headset-001",
) -> SipRegistrationEvent:
    """
    A validated event as telemetry-parser now emits them.

    This used to construct a StructuredEvent — a locally-defined envelope
    with a dict payload, which accepted anything. It is now the schema
    library's type, so a fixture that does not satisfy the contract fails
    here rather than passing a test and failing in production.
    """

    now = datetime.now(timezone.utc)

    return SipRegistrationEvent(
        event_id=uuid4(),
        event_timestamp=now,
        ingest_timestamp=now,
        trace=TraceContext(trace_id=uuid4(), pipeline_stage=PipelineStage.INGESTION),
        metadata=EventMetadata(
            event_type=SipRegistrationEvent.__event_type__,
            schema_version=SipRegistrationEvent.__schema_version__,
            source="telemetry-parser",
        ),
        payload=SipRegistrationPayload(
            device_id=derive_device_id(store_id, device_label),
            device_label=device_label,
            store_id=store_id,
            registration_status=RegistrationStatus.REGISTERED,
            observed_at=now,
        ),
    )


def test_publish_sends_to_correct_topic() -> None:
    client = FakeIPCClient()
    publisher = GreengrassEventPublisher(store_id=STORE_ID, ipc_client=client)

    publisher.publish(make_event())

    assert len(client.published) == 1
    assert client.published[0]["topic"] == "edge/store-0042/telemetry"


def test_publish_uses_at_least_once_qos() -> None:
    client = FakeIPCClient()
    publisher = GreengrassEventPublisher(store_id=STORE_ID, ipc_client=client)

    publisher.publish(make_event())

    assert client.published[0]["qos"] == GreengrassEventPublisher.QOS_AT_LEAST_ONCE


def test_publish_payload_is_valid_json_of_the_event() -> None:
    client = FakeIPCClient()
    publisher = GreengrassEventPublisher(store_id=STORE_ID, ipc_client=client)
    event = make_event()

    publisher.publish(event)

    decoded = json.loads(client.published[0]["payload"].decode("utf-8"))

    assert decoded["event_id"] == str(event.event_id)
    assert decoded["payload"]["store_id"] == STORE_ID
    assert decoded["payload"]["registration_status"] == "REGISTERED"


def test_published_payload_names_its_own_schema() -> None:
    """
    The envelope is self-describing through metadata, so a consumer
    reading the JSON alone can tell what it is without inspecting payload
    keys to guess. Nothing here needs to add that identity.
    """

    client = FakeIPCClient()
    publisher = GreengrassEventPublisher(store_id=STORE_ID, ipc_client=client)

    publisher.publish(make_event())

    decoded = json.loads(client.published[0]["payload"].decode("utf-8"))

    assert decoded["metadata"]["event_type"] == "sip.registration"
    assert decoded["metadata"]["schema_version"] == "v1"


def test_published_payload_is_json_serialisable_without_help() -> None:
    """
    model_dump(mode="json") rather than mode="python": the payload holds
    UUIDs, datetimes and an enum, none of which json.dumps handles.
    """

    client = FakeIPCClient()
    publisher = GreengrassEventPublisher(store_id=STORE_ID, ipc_client=client)

    publisher.publish(make_event())

    json.loads(client.published[0]["payload"].decode("utf-8"))


def test_publish_raises_typed_error_on_ipc_failure() -> None:
    client = FakeIPCClient(fail=True)
    publisher = GreengrassEventPublisher(store_id=STORE_ID, ipc_client=client)

    with pytest.raises(PublishError):
        publisher.publish(make_event())


def test_empty_store_id_rejected() -> None:
    with pytest.raises(ValueError):
        GreengrassEventPublisher(store_id="", ipc_client=FakeIPCClient())


def test_multiple_events_use_same_store_topic() -> None:
    client = FakeIPCClient()
    publisher = GreengrassEventPublisher(store_id="store-0007", ipc_client=client)

    publisher.publish(make_event(store_id="store-0007", device_label="headset-a"))
    publisher.publish(make_event(store_id="store-0007", device_label="headset-b"))

    assert len(client.published) == 2
    assert all(p["topic"] == "edge/store-0007/telemetry" for p in client.published)


# ---------------------------------------------------------------
# store consistency
# ---------------------------------------------------------------

# The topic and the payload both carry a store id, from the same
# controller configuration. If they disagree the controller is
# misconfigured, and the failure is silent: events land on one store's
# topic while claiming another, and each downstream consumer believes
# whichever it happens to read.


def test_event_from_another_store_is_rejected() -> None:
    client = FakeIPCClient()
    publisher = GreengrassEventPublisher(store_id=STORE_ID, ipc_client=client)

    with pytest.raises(StoreMismatchError):
        publisher.publish(make_event(store_id="store-9999"))


def test_mismatched_event_is_not_published() -> None:
    """Rejected before the IPC call, not after it."""

    client = FakeIPCClient()
    publisher = GreengrassEventPublisher(store_id=STORE_ID, ipc_client=client)

    with pytest.raises(StoreMismatchError):
        publisher.publish(make_event(store_id="store-9999"))

    assert client.published == []
