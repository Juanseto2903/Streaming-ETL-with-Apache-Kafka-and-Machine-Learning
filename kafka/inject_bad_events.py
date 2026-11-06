"""Inject deliberately broken events into the topic for consumer resilience tests.

Publishes a FIXED catalog of 10 bad messages. Each one exercises one brand of
failure:

    INVALID_SCHEMA   : malformed JSON, 2 incomplete schemas (missing feature /
                       missing event_id), 1 valid event published with NO Kafka
                       key (contract key = country)
    INVALID_VALUES   : wrong feature type, negative feature, NaN feature, JSON
                       null field, non-ISO event_time, wrong year type

The point is to prove the consumer stores them raw, flags the right status and
KEEPS RUNNING - these rows must never reach fact_predictions.

    python kafka/inject_bad_events.py              # publish the whole catalog
    python kafka/inject_bad_events.py --repeat 2   # publish it twice (ids shift)
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from confluent_kafka import Producer

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

LOGGER = logging.getLogger("inject-bad")

TRACKED = {"delivered": 0, "failed": 0}


def delivery_report(err, msg) -> None:  # noqa: ANN001 (client-fixed callback signature)
    if err is not None:
        LOGGER.error("delivery failed (%s): %s", msg.value()[:20] if msg else "?", err)
        TRACKED["failed"] += 1
    else:
        TRACKED["delivered"] += 1


def bad_catalog(repeat: int) -> list:
    """(payload, kafka_key) tuples; kafka_key=None tests the missing-key case."""
    nan_feature = float("nan")
    catalog = [
        # 1. malformed JSON (truncated object -> json.loads fails; event_id = NULL in raw)
        ('{"event_id": "BAD001", "country": "Testland", "year": 2019, "gdp": 0.9', "Testland"),
        # 2. missing 'freedom' from the required feature set
        (json.dumps({"event_id": "BAD002", "country": "Testland", "year": 2019,
                     "gdp": 0.9, "family": 0.8, "health": 0.7, "generosity": 0.1,
                     "corruption": 0.15, "actual_happiness_score": 5.1,
                     "event_time": "2026-11-05T20:00:00"}), "Testland"),
        # 3. valid event shape but NO event_id at all (raw keeps event_id = NULL)
        (json.dumps({"country": "Testland", "year": 2019,
                     "gdp": 0.9, "family": 0.8, "health": 0.7, "freedom": 0.6,
                     "generosity": 0.1, "corruption": 0.15,
                     "actual_happiness_score": 5.1, "event_time": "2026-11-05T20:05:00"}), "Testland"),
        # 4. wrong type: gdp is text
        (json.dumps({"event_id": "BAD004", "country": "Testland", "year": 2019,
                     "gdp": "high", "family": 0.8, "health": 0.7, "freedom": 0.6,
                     "generosity": 0.1, "corruption": 0.15,
                     "actual_happiness_score": 5.1, "event_time": "2026-11-05T20:10:00"}), "Testland"),
        # 5. negative family
        (json.dumps({"event_id": "BAD005", "country": "Testland", "year": 2019,
                     "gdp": 0.9, "family": -0.5, "health": 0.7, "freedom": 0.6,
                     "generosity": 0.1, "corruption": 0.15,
                     "actual_happiness_score": 5.1, "event_time": "2026-11-05T20:15:00"}), "Testland"),
        # 6. NaN feature (serialized as the NaN literal, parsed back as float nan)
        (json.dumps({"event_id": "BAD006", "country": "Testland", "year": 2019,
                     "gdp": 0.9, "family": 0.8, "health": 0.7, "freedom": 0.6,
                     "generosity": 0.1, "corruption": nan_feature,
                     "actual_happiness_score": 5.1, "event_time": "2026-11-05T20:20:00"}), "Testland"),
        # 7. event_time not ISO-8601
        (json.dumps({"event_id": "BAD007", "country": "Testland", "year": 2019,
                     "gdp": 0.9, "family": 0.8, "health": 0.7, "freedom": 0.6,
                     "generosity": 0.1, "corruption": 0.15,
                     "actual_happiness_score": 5.1, "event_time": "not-a-date"}), "Testland"),
        # 8. year as text
        (json.dumps({"event_id": "BAD008", "country": "Testland", "year": "2019",
                     "gdp": 0.9, "family": 0.8, "health": 0.7, "freedom": 0.6,
                     "generosity": 0.1, "corruption": 0.15,
                     "actual_happiness_score": 5.1, "event_time": "2026-11-05T20:30:00"}), "Testland"),
        # 9. null field: gdp is JSON null (present but not a number -> INVALID_VALUES)
        (json.dumps({"event_id": "BAD009", "country": "Testland", "year": 2019,
                     "gdp": None, "family": 0.8, "health": 0.7, "freedom": 0.6,
                     "generosity": 0.1, "corruption": 0.15,
                     "actual_happiness_score": 5.1, "event_time": "2026-11-05T20:35:00"}), "Testland"),
        # 10. valid complete JSON published with NO Kafka key (violates key = country;
        #     event itself validates fenced -> rejected as INVALID_SCHEMA, no fact row)
        (json.dumps({"event_id": "BAD010", "country": "Testland", "year": 2019,
                     "gdp": 0.9, "family": 0.8, "health": 0.7, "freedom": 0.6,
                     "generosity": 0.1, "corruption": 0.15,
                     "actual_happiness_score": 5.1, "event_time": "2026-11-05T20:40:00"}), None),
    ]
    # NaN must never slip through silently if the serializer changed
    assert math.isnan(nan_feature)
    return catalog * max(repeat, 1)


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Publish broken events for resilience tests.")
    parser.add_argument("--repeat", type=int, default=1, help="Publish the catalog N times.")
    args = parser.parse_args()

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "happiness-events")
    producer = Producer({"bootstrap.servers": bootstrap})

    catalog = bad_catalog(args.repeat)
    for number, (payload, key) in enumerate(catalog, start=1):
        producer.produce(topic=topic, value=payload.encode("utf-8"),
                         key=key.encode("utf-8") if key is not None else None,
                         on_delivery=delivery_report)
        if number % 4 == 0:
            producer.flush(timeout=10)
    remaining = producer.flush(timeout=30)
    LOGGER.info("published %d bad events -> delivered=%d failed=%d (queue left=%d)",
                len(catalog), TRACKED["delivered"], TRACKED["failed"], remaining)
    return 1 if (TRACKED["failed"] or remaining) else 0


if __name__ == "__main__":
    sys.exit(main())
