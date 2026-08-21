# greengrass-publisher

Publishes `telemetry-parser`'s `StructuredEvent` instances to AWS IoT
Core over the Greengrass Core IPC daemon — the link between the edge
SIP listener and SignalForge's ingestion pipeline (EventBridge → SQS →
Lambda → detection/dashboards/dataset export).

Part of SignalForge, alongside `event-schema-contracts`,
`structured-logging-python`, `telemetry-parser`, `aws-event-pipeline-infra`,
and `signal-forge` itself.

## Why a separate repo

Transport is a distinct concern from parsing, with an independent
release cadence (Greengrass/awsiot SDK changes have nothing to do with
parsing logic) and — matching the precedent already set by
`structured-logging-python` — small, focused, reusable components in
SignalForge get their own repo rather than living inside a larger
one.

## Install

Not published to PyPI. Depends on `telemetry-parser` and
`structured-logging-python` being present on `PYTHONPATH` — see CI
workflow for the exact pattern.

```bash
pip install -e ".[dev]"
```

## Usage

```python
from structured_logging.core.context import ServiceContext
from telemetry_parser.output.event_emitter import EventEmitter
from greengrass_publisher.publisher import (
    GreengrassEventPublisher,
    build_default_ipc_client,
)

# Once, at process startup:
ServiceContext.initialise(service_name="greengrass-publisher", environment="production")

publisher = GreengrassEventPublisher(
    store_id="store-0042",
    ipc_client=build_default_ipc_client(),  # real client — needs the
                                              # awsiot SDK, only installable
                                              # on a Greengrass device/simulator:
                                              #   pip install awsiotsdk
)
emitter = EventEmitter(on_emit=publisher.publish)
```

Logs via `structured_logging.StructuredLogger`, matching every other
SignalForge service, with each log line carrying the originating
event's `trace_id` for cross-service correlation.

## Design decisions

- **Greengrass IPC, not a raw MQTT/TLS connection** — the device's
  connection to AWS is already managed by the Greengrass Core daemon;
  components publish to it locally over IPC.
- **QoS AT_LEAST_ONCE** — telemetry tolerates an occasional duplicate
  (downstream dedupes by `event_id`) but not silent loss.
- **Topic**: `edge/{store_id}/telemetry` — a single wildcard IoT Core
  rule (`edge/+/telemetry`) can route every store's traffic without
  per-store configuration.

## Testing

```bash
pytest -v
```

Tests use `FakeIPCClient` in place of the real Greengrass IPC
connection — no AWS dependency required.

## What this doesn't do yet

The IoT Core topic rule (`edge/+/telemetry` → EventBridge) that
receives what this publishes isn't built — belongs in
`aws-event-pipeline-infra` as a new Terraform module.
