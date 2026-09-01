"""
Publishes validated telemetry events onto AWS IoT Core via the local
Greengrass IPC daemon, as an `on_emit` callback for telemetry_parser's
EventEmitter.

Design decisions (see docs/design-notes.md for the fuller rationale):

- Publishes via Greengrass IPC (PublishToIoTCore), not a raw MQTT/TLS
  connection — the device's connection to AWS is already managed by
  the Greengrass Core daemon; components talk to it locally over IPC
  rather than each opening their own connection to the cloud.
- QoS AT_LEAST_ONCE — telemetry can tolerate an occasional duplicate
  (downstream events are deduplicated by event_id), but not silent
  loss. AT_MOST_ONCE would risk silent loss; EXACTLY_ONCE isn't a
  supported MQTT 3.1.1 QoS level and adds cost this data doesn't need.
- Topic includes store_id so a single IoT Core rule can subscribe with
  a wildcard (`edge/+/telemetry`) and route every store's traffic
  through one rule, rather than needing per-store configuration.
- The store_id given here and the one inside each event both come from
  the controller's provisioned configuration, so they should always
  agree. `publish` checks that they do. If they disagree the controller
  is misconfigured, and the failure is otherwise invisible: events land
  on one store's topic while claiming to belong to another, and every
  downstream consumer believes whichever it happens to read.
- Logs via structured_logging.StructuredLogger, matching every other
  service in SignalForge, with each log line carrying the originating
  event's trace_id — NOT stdlib logging. Requires the caller to have
  already run:

      from structured_logging.core.context import ServiceContext
      ServiceContext.initialise(service_name="greengrass-publisher",
                                 environment="production")

  once at process startup, before any event is published — this class
  does not call initialise() itself, since environment/service naming
  is an application-wireup concern, not this module's.
"""

from __future__ import annotations

import json
from typing import Protocol

from event_schema_contracts.telemetry.sip_registration_event import (
    SipRegistrationEvent,
)
from structured_logging.core.logger import StructuredLogger

logger = StructuredLogger(__name__)

TOPIC_TEMPLATE = "edge/{store_id}/telemetry"


class PublishError(Exception):
    """Raised when an event could not be published to IoT Core."""


class StoreMismatchError(Exception):
    """
    Raised when an event claims a different store from the one this
    publisher was configured for.

    Both values come from the same controller configuration, so a
    mismatch is a wiring error rather than a data condition — worth
    failing on rather than resolving silently in either direction.
    """


class GreengrassIPCClient(Protocol):
    """
    The subset of the Greengrass Core IPC client this publisher needs.
    Defined as a Protocol so tests can supply a fake without importing
    the real awsiot SDK (which isn't installable outside a Greengrass
    device/simulator environment).
    """

    def publish_to_iot_core(self, topic: str, payload: bytes, qos: int) -> None: ...


class GreengrassEventPublisher:
    """
    Publishes validated telemetry events to IoT Core over Greengrass IPC.

    Usage as telemetry_parser's EventEmitter callback:

        publisher = GreengrassEventPublisher(store_id="store-0042")
        emitter = EventEmitter(on_emit=publisher.publish)

    Events are `SipRegistrationEvent` from `event-schema-contracts`. The
    parser previously emitted a locally-defined `StructuredEvent`, which
    was removed when it began constructing the schema library's types
    directly — so the schema validates the parser's output at the point
    it is produced rather than a consumer discovering a mismatch.
    """

    # MQTT QoS 1 — "at least once". See module docstring for rationale.
    QOS_AT_LEAST_ONCE = 1

    def __init__(
        self,
        store_id: str,
        ipc_client: GreengrassIPCClient,
        topic_template: str = TOPIC_TEMPLATE,
    ) -> None:
        if not store_id:
            raise ValueError("store_id cannot be empty")

        self._store_id = store_id
        self._ipc_client = ipc_client
        self._topic = topic_template.format(store_id=store_id)

    def publish(self, event: SipRegistrationEvent) -> None:
        """
        Publish a single event. Matches the signature EventEmitter's
        `on_emit` callback expects: callable(event) -> None.

        Raises PublishError on failure rather than swallowing it —
        the caller (the parser pipeline) decides whether a publish
        failure should halt processing, be retried, or be logged and
        skipped; this class doesn't make that policy decision.
        """
        if event.payload.store_id != self._store_id:
            raise StoreMismatchError(
                f"event claims store {event.payload.store_id!r} but this "
                f"publisher is configured for {self._store_id!r}"
            )

        payload_bytes = json.dumps(event.model_dump(mode="json")).encode("utf-8")

        try:
            self._ipc_client.publish_to_iot_core(
                topic=self._topic,
                payload=payload_bytes,
                qos=self.QOS_AT_LEAST_ONCE,
            )
        except Exception as exc:  # noqa: BLE001 — re-raised as a typed error below
            logger.emit_error(
                error_code="GREENGRASS_PUBLISH_FAILED",
                message=f"Failed to publish event to {self._topic}",
                event_type="publish.failed",
                metadata={"event_id": str(event.event_id), "topic": self._topic},
                trace_id=str(event.trace.trace_id),
                exception_type=type(exc).__name__,
                retryable=True,
            )
            raise PublishError(
                f"Failed to publish event {event.event_id} to {self._topic}"
            ) from exc

        logger.debug(
            "Published event to IoT Core",
            event_type="publish.succeeded",
            metadata={
                "event_id": str(event.event_id),
                "source_event_type": event.metadata.event_type,
                "topic": self._topic,
            },
            trace_id=str(event.trace.trace_id),
        )


def build_default_ipc_client() -> GreengrassIPCClient:
    """
    Constructs the real Greengrass IPC client, wrapped to match the
    GreengrassIPCClient protocol above. Deferred import: the awsiot
    package is only installable in a Greengrass device/simulator
    environment, so importing it here (rather than at module load
    time) keeps this module importable — and testable — anywhere.
    """
    import awsiot.greengrasscoreipc
    from awsiot.greengrasscoreipc.model import QOS, PublishToIoTCoreRequest

    class _RealGreengrassIPCClient:
        def __init__(self) -> None:
            self._client = awsiot.greengrasscoreipc.connect()

        def publish_to_iot_core(self, topic: str, payload: bytes, qos: int) -> None:
            request = PublishToIoTCoreRequest()
            request.topic_name = topic
            request.payload = payload
            request.qos = QOS.AT_LEAST_ONCE if qos == 1 else QOS.AT_MOST_ONCE

            operation = self._client.new_publish_to_iot_core()
            operation.activate(request)
            operation.get_response().result(timeout=5)

    return _RealGreengrassIPCClient()
