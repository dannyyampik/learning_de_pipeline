"""Clickstream production: Avro-serialized events onto Kafka.

The producer registers the event schema with Schema Registry on first use;
after that every message carries just a 5-byte schema id + Avro binary —
the standard "schemas on the wire" contract between producers & consumers.
"""

import logging
import random
import time
import uuid
from pathlib import Path

from confluent_kafka import Producer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

log = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).with_name("clickstream_event_v1.avsc")


class ClickstreamProducer:
    def __init__(self, cfg):
        self.cfg = cfg
        schema_str = SCHEMA_PATH.read_text()
        sr_client = SchemaRegistryClient({"url": cfg.schema_registry_url})
        self._producer = SerializingProducer(
            {
                "bootstrap.servers": cfg.kafka_bootstrap,
                "key.serializer": StringSerializer("utf_8"),
                "value.serializer": AvroSerializer(sr_client, schema_str),
                # At-least-once-friendly defaults; exactly-once is a later lesson
                "enable.idempotence": True,
                "linger.ms": 50,
            }
        )
        # Separate raw producer used only to inject malformed ("poison")
        # messages that bypass Avro — fodder for DLQ handling in phase 3.
        self._raw_producer = Producer({"bootstrap.servers": cfg.kafka_bootstrap})
        self.emitted = 0
        self.malformed = 0

    def emit(self, event: dict) -> None:
        # A slice of events arrives late (mobile clients, flaky networks):
        # event_ts drifts up to 5 minutes into the past while the message
        # itself is produced now. Watermark lessons need real late data.
        if random.random() < self.cfg.late_event_pct:
            event["event_ts"] -= random.randint(60_000, 300_000)

        if random.random() < self.cfg.bad_event_pct:
            self._raw_producer.produce(
                self.cfg.clickstream_topic,
                key=event["session_id"].encode(),
                value=b'{"oops": "not avro at all"}',
            )
            self.malformed += 1
        else:
            self._producer.produce(
                topic=self.cfg.clickstream_topic,
                key=event["session_id"],
                value=event,
            )
            self.emitted += 1
        # Serve delivery callbacks without blocking the tick loop
        self._producer.poll(0)
        self._raw_producer.poll(0)

    def flush(self) -> None:
        self._producer.flush(5)
        self._raw_producer.flush(5)


def base_event(session, event_type: str, page_url: str, properties: dict | None = None) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_ts": int(time.time() * 1000),
        "session_id": session.session_id,
        "customer_id": session.customer_id,
        "device": session.device,
        "page_url": page_url,
        "referrer": session.referrer,
        "properties": {k: str(v) for k, v in (properties or {}).items()},
    }
