import json
from datetime import datetime, timezone

import pytest

from greengrass_publisher.publisher import (
    GreengrassEventPublisher,
    PublishError,
)
from telemetry_parser.output.structured_event import StructuredEvent


class FakeIPCClient:
    """Records every publish call instead of talking to real IoT Core."""

    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.published: list[dict] = []

    def publish_to_iot_core(self, topic: str, payload: bytes, qos: int) -> None:
        if self.fail:
            raise RuntimeError("simulated IPC failure")
        self.published.append({"topic": topic, "payload": payload, "qos": qos})


def make_event(event_id: str = "evt-001") -> StructuredEvent:
    now = datetime.now(timezone.utc)
    return StructuredEvent(
        schema_version="1.0",
        event_id=event_id,
        trace_id="trace-001",
        event_timestamp=now,
        ingest_timestamp=now,
        event_type="telemetry.registration",
        source="telemetry-parser",
        payload={"status": "registered", "latency_ms": 42},
    )


def test_publish_sends_to_correct_topic() -> None:
    client = FakeIPCClient()
    publisher = GreengrassEventPublisher(store_id="store-0042", ipc_client=client)

    publisher.publish(make_event())

    assert len(client.published) == 1
    assert client.published[0]["topic"] == "edge/store-0042/telemetry"


def test_publish_uses_at_least_once_qos() -> None:
    client = FakeIPCClient()
    publisher = GreengrassEventPublisher(store_id="store-0042", ipc_client=client)

    publisher.publish(make_event())

    assert client.published[0]["qos"] == GreengrassEventPublisher.QOS_AT_LEAST_ONCE


def test_publish_payload_is_valid_json_of_the_event() -> None:
    client = FakeIPCClient()
    publisher = GreengrassEventPublisher(store_id="store-0042", ipc_client=client)
    event = make_event(event_id="evt-002")

    publisher.publish(event)

    decoded = json.loads(client.published[0]["payload"].decode("utf-8"))
    assert decoded["event_id"] == "evt-002"
    assert decoded["event_type"] == "telemetry.registration"
    assert decoded["payload"]["status"] == "registered"


def test_publish_raises_typed_error_on_ipc_failure() -> None:
    client = FakeIPCClient(fail=True)
    publisher = GreengrassEventPublisher(store_id="store-0042", ipc_client=client)

    with pytest.raises(PublishError):
        publisher.publish(make_event())


def test_empty_store_id_rejected() -> None:
    with pytest.raises(ValueError):
        GreengrassEventPublisher(store_id="", ipc_client=FakeIPCClient())


def test_multiple_events_use_same_store_topic() -> None:
    client = FakeIPCClient()
    publisher = GreengrassEventPublisher(store_id="store-0007", ipc_client=client)

    publisher.publish(make_event("evt-a"))
    publisher.publish(make_event("evt-b"))

    assert len(client.published) == 2
    assert all(p["topic"] == "edge/store-0007/telemetry" for p in client.published)
