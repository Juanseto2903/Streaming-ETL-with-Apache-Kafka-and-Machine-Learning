"""Kafka producer for the streaming happiness pipeline (Part B, Step 5).

Reads the 235-row test split (data/streaming/test_split.csv), builds one JSON
event per row following the event contract, and publishes them to the
'happiness-events' topic with the country as the message key (so all events of
a country land on the same partition and stay ordered per country).

Every setting comes from environment variables (.env, loaded with
python-dotenv); command-line flags (--delay, --limit) override them for a
single run without touching the environment.

Run from the repo root with the project virtualenv:
    python kafka/producer.py
    python kafka/producer.py --delay 0.2 --limit 10
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# ---------------------------------------------------------------------------
# Paths: everything is resolved relative to the REPO ROOT, so the script can
# be launched from any working directory.
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

LOGGER = logging.getLogger("producer")

# Event contract fields carried in every message (lhs = JSON key, rhs = split
# column). 'actual_happiness_score' comes from the split's 'happiness_score'
# column; it is only for later evaluation, never a model feature.
FEATURE_COLUMNS = ("gdp", "family", "health", "freedom", "generosity", "corruption")

# Delivery callback bookkeeping (filled by delivery_report()).
TRACKED = {"delivered": 0, "failed": 0}


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream the test split to Kafka.")
    parser.add_argument("--delay", type=float, default=None,
                        help="Seconds between messages (overrides PRODUCER_DELAY_SECONDS).")
    parser.add_argument("--limit", type=int, default=None,
                        help="Stream at most N events this run (default: all rows).")
    return parser.parse_args()


def load_config(args: argparse.Namespace) -> dict:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "happiness-events")
    key_source = os.getenv("KAFKA_KEY_SOURCE", "country")
    delay_env = os.getenv("PRODUCER_DELAY_SECONDS", "1.0")
    data_path_env = os.getenv("STREAMING_DATA_PATH", "data/streaming/test_split.csv")

    delay = float(args.delay) if args.delay is not None else float(delay_env)
    if delay < 0:
        raise ValueError(f"delay must be >= 0, got {delay}")
    if args.limit is not None and args.limit <= 0:
        raise ValueError(f"--limit must be a positive integer, got {args.limit}")

    data_path = Path(data_path_env)
    if not data_path.is_absolute():  # project paths are relative to the repo root
        data_path = ROOT / data_path

    return {"bootstrap": bootstrap, "topic": topic, "key_source": key_source,
            "delay": delay, "limit": args.limit, "data_path": data_path}


# ---------------------------------------------------------------------------
# Topic management: create the topic with 3 partitions / replication factor 1
# if missing, and refuse to run against an under-partitioned existing one.
# ---------------------------------------------------------------------------
def ensure_topic(admin: AdminClient, topic: str) -> None:
    try:
        result = admin.create_topics([NewTopic(
            topic=topic, num_partitions=3, replication_factor=1)])
        for name, future in result.items():
            future.result(timeout=30)
        LOGGER.info("topic '%s' created (3 partitions, replication factor 1)", topic)
    except Exception as exc:
        if "ALREADY_EXISTS" not in str(exc):
            raise

    # Whether it was just created or already existed, this run REQUIRES 3 partitions.
    partitions = admin.list_topics(topic=topic).topics[topic].partitions
    if len(partitions) != 3:
        raise RuntimeError(f"topic '{topic}' has {len(partitions)} partitions, expected 3")


# ---------------------------------------------------------------------------
# Contract events from the split. Columns are selected BY NAME; the unified
# schema/order is already guaranteed by the notebook that generated the file.
# ---------------------------------------------------------------------------
REQUIRED_COLUMNS = ("country", "year", "happiness_score") + FEATURE_COLUMNS


def build_event(row_number: int, row: dict, key_source: str, emitted_at: str) -> tuple:
    """Turn one raw CSV row dict into an (key, payload) pair."""
    event = {
        "event_id": f"E{row_number:03d}",
        "country": str(row["country"]).strip(),
        "year": int(row["year"]),
    }
    for feature in FEATURE_COLUMNS:
        event[feature] = float(row[feature])
    event["actual_happiness_score"] = float(row["happiness_score"])
    event["event_time"] = emitted_at

    key = event.get(key_source)
    if key is None:
        raise KeyError(f"event E{row_number:03d}: key column '{key_source}' not in the event")
    if not str(key).strip():
        raise ValueError(f"event E{row_number:03d}: empty '{key_source}' (message key)")
    # allow_nan=False: an accidental NaN/inf must raise HERE (caught by the
    # per-row try/except in stream()), not travel as invalid JSON to the topic
    return str(key), json.dumps(event, ensure_ascii=False, allow_nan=False).encode("utf-8")


# ---------------------------------------------------------------------------
# Delivery callback: confluent-kafka invokes it once per message, either with
# an error or with the (topic, partition, offset) it finally landed on.
# The event id is parsed back out of the payload so the log names the contract
# id (E001...) instead of slicing the raw serialized JSON.
# ---------------------------------------------------------------------------
def _event_id_of(payload: bytes) -> str:
    try:
        return json.loads(payload.decode("utf-8")).get("event_id", "?")
    except Exception:
        return "?"


def delivery_report(err, msg) -> None:  # noqa: ANN001 (callback signature fixed by the client)
    if err is not None:
        LOGGER.error("delivery failed for %s[%s] @ offset %s (id=%s): %s",
                     msg.topic(), msg.partition(), msg.offset(),
                     _event_id_of(msg.value()), err)
        TRACKED["failed"] += 1
    else:
        LOGGER.info("delivered %s[%s] @ offset %s (id=%s)",
                    msg.topic(), msg.partition(), msg.offset(),
                    _event_id_of(msg.value()))
        TRACKED["delivered"] += 1


# ---------------------------------------------------------------------------
def stream(config: dict) -> None:
    frame_path = config["data_path"]
    if not frame_path.is_file():
        raise FileNotFoundError(f"streaming data not found: {frame_path}")

    import pandas as pd
    frame = pd.read_csv(frame_path)
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise KeyError(f"streaming data missing columns: {missing}")
    LOGGER.info("streaming data loaded: %d rows (%s)", len(frame), frame_path.as_posix())

    producer = Producer({"bootstrap.servers": config["bootstrap"],
                         "enable.idempotence": True,  # no duplicates in re-tries
                         "linger.ms": 5})

    events = enumerate(frame.to_dict("records"), start=1)
    for row_number, row in events:
        if config["limit"] is not None and row_number > config["limit"]:
            break
        try:
            key, payload = build_event(row_number, row, config["key_source"],
                                       datetime.now().isoformat(timespec="seconds"))
            producer.produce(topic=config["topic"], value=payload,
                             key=key.encode("utf-8"), on_delivery=delivery_report)
        except Exception:
            LOGGER.exception("event #%d skipped", row_number)
            TRACKED["failed"] += 1
        producer.poll(0)          # serve delivery callbacks while streaming
        if row_number < (config["limit"] if config["limit"] else len(frame)):
            time.sleep(config["delay"])

    remaining = producer.flush(timeout=30)  # everything must actually land
    while remaining > 0:
        remaining = producer.flush(timeout=30)


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    config = load_config(parse_args())
    LOGGER.info("config: bootstrap=%s topic=%s delay=%.2fs data=%s",
                config["bootstrap"], config["topic"], config["delay"],
                config["data_path"].as_posix())

    ensure_topic(AdminClient({"bootstrap.servers": config["bootstrap"]}), config["topic"])
    stream(config)

    LOGGER.info("run finished: delivered=%d failed=%d", TRACKED["delivered"], TRACKED["failed"])
    return 1 if TRACKED["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
