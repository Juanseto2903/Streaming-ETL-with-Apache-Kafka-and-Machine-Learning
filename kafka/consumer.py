"""Kafka consumer for the streaming happiness pipeline (Part B, Step 6 + Part C, Step 8).

Pipeline per message, in strict order:
  1. STORE the raw message FIRST (raw_happiness_events, status PENDING,
     payload as the exact UTF-8 string received - malformed JSON must still be
     stored, which is why the column is LONGTEXT and not JSON).
  2. VALIDATE against the event contract. Missing schema pieces ->
     INVALID_SCHEMA; present but semantically wrong (wrong types, negative /
     NaN / inf features, non-ISO timestamps) -> INVALID_VALUES.
  3. PREDICT with the trained sklearn Pipeline (models/model.pkl), features in
     the exact training order. Contract note: 'actual_happiness_score' is only
     an evaluation field - it is NEVER a model feature.
  4. ANALYTICAL storage: upsert dim_country + dim_date, insert fact_predictions
     (unique by event_id, so re-processing never inflates the KPIs) linked to
     the raw row, and mark the raw row VALID.
  5. Only after the database write commits is the Kafka offset committed
     (at-least-once delivery). A prediction that fails for an unexpected reason
     leaves a PREDICTION_ERROR status, and invalid events are skipped without
     ever stopping the loop.

Settings come from environment variables (.env via python-dotenv). Useful CLI
flags for unattended tests:
    python kafka/consumer.py --timeout 30          # stop after 30 s idle
    python kafka/consumer.py --max-messages 10     # stop after 10 messages
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import joblib
import pandas as pd
import pymysql
from dotenv import load_dotenv
from confluent_kafka import Consumer

# ---------------------------------------------------------------------------
# Paths and contract constants: everything resolved relative to the REPO ROOT;
# the event fields below are frozen to match the training feature order.
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

LOGGER = logging.getLogger("consumer")

FEATURES = ["gdp", "family", "health", "freedom", "generosity", "corruption"]
REQUIRED_FIELDS = ("event_id", "country", "year", "actual_happiness_score", "event_time")

STATUS_PENDING = "PENDING"
STATUS_VALID = "VALID"
STATUS_INVALID_SCHEMA = "INVALID_SCHEMA"
STATUS_INVALID_VALUES = "INVALID_VALUES"
STATUS_PREDICTION_ERROR = "PREDICTION_ERROR"

COUNTERS = {"consumed": 0, "VALID": 0, "INVALID_SCHEMA": 0,
            "INVALID_VALUES": 0, "PREDICTION_ERROR": 0, "failed": 0}


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume, validate, predict and store happiness events.")
    parser.add_argument("--max-messages", type=int, default=None,
                        help="Stop after processing N messages (default: unlimited).")
    parser.add_argument("--timeout", type=float, default=None,
                        help="Stop after S seconds without messages (default: run forever).")
    return parser.parse_args()


def load_config(args: argparse.Namespace) -> dict:
    model_path_env = os.getenv("MODEL_PATH", "models/model.pkl")
    model_path = Path(model_path_env)
    if not model_path.is_absolute():
        model_path = ROOT / model_path
    if args.max_messages is not None and args.max_messages <= 0:
        raise ValueError("--max-messages must be a positive integer")

    return {
        "bootstrap": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "topic": os.getenv("KAFKA_TOPIC", "happiness-events"),
        "group": os.getenv("KAFKA_CONSUMER_GROUP", "happiness-ml-team"),
        "timeout": args.timeout,
        "max_messages": args.max_messages,
        "model_path": model_path,
        "mysql": {
            "host": os.getenv("MYSQL_HOST", "localhost"),
            "port": int(os.getenv("MYSQL_PORT", "3306")),
            "database": os.getenv("MYSQL_DATABASE", "happiness_db"),
            "user": os.getenv("MYSQL_USER", "happiness_user"),
            "password": os.getenv("MYSQL_PASSWORD", ""),
        },
    }


# ---------------------------------------------------------------------------
# One bootstrap per long-lived object
# ---------------------------------------------------------------------------
def load_model(model_path: Path):
    if not model_path.is_file():
        raise FileNotFoundError(f"model not found: {model_path}")
    pipeline = joblib.load(model_path)
    if list(getattr(pipeline, "feature_names_in_", [])) != FEATURES:
        raise RuntimeError(
            f"model expects features {list(getattr(pipeline, 'feature_names_in_', []))}, "
            f"consumer is contract-aligned to {FEATURES} - refusing to run")
    LOGGER.info("model loaded (%s, %d bytes)", model_path.as_posix(), model_path.stat().st_size)
    return pipeline


def connect_mysql(cfg: dict) -> pymysql.connections.Connection:
    connection = pymysql.connect(
        host=cfg["host"], port=cfg["port"],
        user=cfg["user"], password=cfg["password"],
        database=cfg["database"], charset="utf8mb4",
        autocommit=False, cursorclass=pymysql.cursors.DictCursor)
    with connection.cursor() as cursor:
        cursor.execute("SELECT VERSION()")
        LOGGER.info("MySQL connected: v%s @ %s:%s/%s", cursor.fetchone()["VERSION()"],
                    cfg["host"], cfg["port"], cfg["database"])
    return connection


def build_consumer(cfg: dict) -> Consumer:
    consumer = Consumer({
        "bootstrap.servers": cfg["bootstrap"],
        "group.id": cfg["group"],
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,   # offsets are committed ONLY after the DB write
    })
    return consumer


# ---------------------------------------------------------------------------
# Step 1: store the raw message BEFORE anything else
# ---------------------------------------------------------------------------
def store_raw(db, payload: str, topic: str, partition: int, offset: int) -> int:
    """Insert the message exactly as received; returns the raw row id.

    Re-delivery of the same broker coordinates (offset replay, service restart
    with earlier committed offsets) must not duplicate the broker record: the
    unique (topic, partition, offset) key is reused so the existing raw row is
    re-linked instead.
    """
    with db.cursor() as cursor:
        try:
            cursor.execute(
                "INSERT INTO raw_happiness_events "
                "(event_id, raw_payload, kafka_topic, kafka_partition, kafka_offset) "
                "VALUES (NULL, %s, %s, %s, %s)",
                (payload, topic, partition, offset))
            db.commit()
            return int(cursor.lastrowid)
        except pymysql.err.IntegrityError as exc:
            if exc.args[0] != 1062:        # not a duplicate-key case
                raise
            db.rollback()
            cursor.execute(
                "SELECT id FROM raw_happiness_events "
                "WHERE kafka_topic = %s AND kafka_partition = %s AND kafka_offset = %s",
                (topic, partition, offset))
            existing = cursor.fetchone()
            if existing is None:           # extremely unlikely race: give up cleanly
                raise
            LOGGER.debug("broker coords already stored as raw id %s - reusing", existing["id"])
            db.commit()
            return int(existing["id"])


def finalize_raw(db, raw_id: int, status: str, event_id: str | None, error_message: str | None) -> None:
    with db.cursor() as cursor:
        cursor.execute(
            "UPDATE raw_happiness_events "
            "SET processing_status = %s, event_id = %s, error_message = %s "
            "WHERE id = %s",
            (status, event_id, error_message, raw_id))
    db.commit()


# ---------------------------------------------------------------------------
# Step 2: contract validation. Returns (data, None) or (None, (status, reason)).
# data = {event_id, country, year, features (dict in FEATURES order),
#         actual_score, event_time}.
# ---------------------------------------------------------------------------
def validate_event(event: dict) -> tuple[dict | None, tuple[str, str] | None]:
    if not isinstance(event, dict):
        return None, (STATUS_INVALID_SCHEMA, "event is not a JSON object")
    missing = [field for field in REQUIRED_FIELDS + tuple(FEATURES) if field not in event]
    if missing:
        return None, (STATUS_INVALID_SCHEMA, f"missing required fields: {missing}")

    if not isinstance(event["event_id"], str) or not event["event_id"].strip():
        return None, (STATUS_INVALID_SCHEMA, "event_id must be a non-empty string")
    if not isinstance(event["country"], str) or not event["country"].strip():
        return None, (STATUS_INVALID_SCHEMA, "country must be a non-empty string")

    # value-level checks: wrong types / non-finite / negative / non-ISO dates
    year = event["year"]
    if isinstance(year, bool) or not isinstance(year, int):
        return None, (STATUS_INVALID_VALUES, f"year must be an integer, got {type(year).__name__}")

    for name in FEATURES + ["actual_happiness_score"]:
        value = event[name]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None, (STATUS_INVALID_VALUES,
                          f"{name} must be a real number, got {type(value).__name__}")
        if not math.isfinite(float(value)):
            return None, (STATUS_INVALID_VALUES, f"{name} is not finite (NaN or inf)")
        if float(value) < 0.0:
            return None, (STATUS_INVALID_VALUES, f"{name} is negative")

    emitted_at = event["event_time"]
    if not isinstance(emitted_at, str):
        return None, (STATUS_INVALID_VALUES,
                      f"event_time must be an ISO-8601 string, got {type(emitted_at).__name__}")
    try:
        event_time = datetime.fromisoformat(emitted_at)
    except ValueError:
        return None, (STATUS_INVALID_VALUES, f"event_time is not a valid ISO-8601 timestamp: {emitted_at!r}")

    return {
        "event_id": event["event_id"].strip(),
        "country": event["country"].strip(),
        "year": int(year),
        "features": {name: float(event[name]) for name in FEATURES},
        "actual_score": float(event["actual_happiness_score"]),
        "event_time": event_time,
    }, None


# ---------------------------------------------------------------------------
# Step 3: inference (features ONLY, in the training order)
# ---------------------------------------------------------------------------
def predict_score(model, features: dict) -> float:
    frame = pd.DataFrame([features], columns=FEATURES)
    return float(model.predict(frame)[0])


# ---------------------------------------------------------------------------
# Step 4: analytical storage (dimensions, idempotent fact insert) + VALID mark
# ---------------------------------------------------------------------------
def upsert_country(db, country_name: str) -> int:
    with db.cursor() as cursor:
        cursor.execute(
            "INSERT INTO dim_country (country_name) VALUES (%s) "
            "ON DUPLICATE KEY UPDATE country_name = VALUES(country_name)",
            (country_name,))
        cursor.execute("SELECT country_id FROM dim_country WHERE country_name = %s",
                       (country_name,))
        return int(cursor.fetchone()["country_id"])


def upsert_date(db, event_time: datetime) -> int:
    date_id = int(event_time.strftime("%Y%m%d"))
    with db.cursor() as cursor:
        cursor.execute(
            "INSERT INTO dim_date (date_id, full_date, year, month, day, day_of_week) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE full_date = VALUES(full_date)",
            (date_id, event_time.date(), event_time.year, event_time.month,
             event_time.day, event_time.strftime("%A")))
        return date_id


def insert_fact(db, raw_id: int, event: dict, predicted_score: float) -> None:
    prediction_error = event["actual_score"] - predicted_score
    with db.cursor() as cursor:
        cursor.execute(
            "INSERT INTO fact_predictions "
            "(raw_event_id, event_id, country_id, date_id, report_year, "
            " actual_score, predicted_score, prediction_error, event_time) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE "
            " raw_event_id = VALUES(raw_event_id), country_id = VALUES(country_id), "
            " date_id = VALUES(date_id), report_year = VALUES(report_year), "
            " actual_score = VALUES(actual_score), predicted_score = VALUES(predicted_score), "
            " prediction_error = VALUES(prediction_error), event_time = VALUES(event_time)",
            (raw_id, event["event_id"], event["country_id"], event["date_id"],
             event["year"], event["actual_score"], predicted_score,
             prediction_error, event["event_time"]))


# ---------------------------------------------------------------------------
# The full message pipeline (never raises: the loop must survive anything).
# Returns True ONLY when the database write/update actually committed, so the
# caller can commit that Kafka offset at-least-once. False = do NOT commit.
# ---------------------------------------------------------------------------
def process_message(db, model, message) -> bool:
    topic, partition, offset = message.topic(), message.partition(), message.offset()

    # non-fatal broker-level errors: nothing is stored, offset is NOT committed
    if error := message.error():
        LOGGER.warning("message-level error on %s[%s]: %s", topic, partition, error)
        return False

    # the payload is the exact UTF-8 string received; a null value (tombstone)
    # is also stored raw FIRST, as an empty payload, which fails JSON parsing
    raw_bytes = message.value()
    payload = "" if raw_bytes is None else raw_bytes.decode("utf-8", errors="replace")

    # 1) store raw BEFORE anything else; a failure here keeps the offset for
    #    redelivery (no commit) so the row is not lost
    try:
        raw_id = store_raw(db, payload, topic, partition, offset)
    except Exception:  # noqa: BLE001 - DB write failed: leave offset uncommitted
        db.rollback()
        COUNTERS["failed"] += 1
        LOGGER.exception("failed to store raw %s[%s] - offset NOT committed", topic, partition)
        return False
    event_id = None

    try:
        # 2) validation
        try:
            parsed = json.loads(payload)
        except ValueError as exc:
            finalize_raw(db, raw_id, STATUS_INVALID_SCHEMA, None,
                         f"message is not valid JSON: {exc}")
            COUNTERS["INVALID_SCHEMA"] += 1
            LOGGER.info("%s[%s] rejected: not valid JSON (raw_id=%s)", topic, partition, raw_id)
            return True
        if not isinstance(parsed, dict):
            finalize_raw(db, raw_id, STATUS_INVALID_SCHEMA, None,
                         f"expected a JSON object, got {type(parsed).__name__}")
            COUNTERS["INVALID_SCHEMA"] += 1
            return True
        if isinstance(parsed.get("event_id"), str):
            event_id = parsed["event_id"].strip() or None

        event, rejected = validate_event(parsed)
        if rejected:
            finalize_raw(db, raw_id, rejected[0], event_id, rejected[1])
            COUNTERS[rejected[0]] += 1
            LOGGER.info("%s[%s] rejected (%s): %s (raw_id=%s)",
                        topic, partition, rejected[0], rejected[1], raw_id)
            return True

        # 2b) Kafka message key check, AFTER event validation so field-level
        #     problems keep precedence: the contract says key = country, and a
        #     broken key must never carry an event into fact_predictions
        raw_key = message.key()
        kafka_key = None if raw_key is None else raw_key.decode("utf-8", errors="replace").strip()
        if kafka_key is None or not kafka_key:
            finalize_raw(db, raw_id, STATUS_INVALID_SCHEMA, event_id, "missing Kafka message key")
            COUNTERS["INVALID_SCHEMA"] += 1
            LOGGER.info("%s[%s] rejected (INVALID_SCHEMA): missing Kafka message key (raw_id=%s)",
                        topic, partition, raw_id)
            return True
        if kafka_key != event["country"]:
            finalize_raw(db, raw_id, STATUS_INVALID_SCHEMA, event_id,
                         f"Kafka message key does not match event country: "
                         f"{kafka_key!r} != {event['country']!r}")
            COUNTERS["INVALID_SCHEMA"] += 1
            LOGGER.info("%s[%s] rejected (INVALID_SCHEMA): key/country mismatch %r != %r (raw_id=%s)",
                        topic, partition, kafka_key, event["country"], raw_id)
            return True

        # 3) prediction (bad rows already skipped - never graded blindly)
        try:
            predicted_score = predict_score(model, event["features"])
        except Exception as exc:  # model-level surprise: log, flag, keep running
            finalize_raw(db, raw_id, STATUS_PREDICTION_ERROR, event_id,
                         f"prediction failed: {exc.__class__.__name__}: {exc}")
            COUNTERS["PREDICTION_ERROR"] += 1
            LOGGER.exception("%s[%s] prediction failed (raw_id=%s)", topic, partition, raw_id)
            return True

        # 4) analytical storage + final VALID mark, one atomic transaction
        event["country_id"] = upsert_country(db, event["country"])
        event["date_id"] = upsert_date(db, event["event_time"])
        insert_fact(db, raw_id, event, predicted_score)
        finalize_raw(db, raw_id, STATUS_VALID, event_id, None)

        COUNTERS["VALID"] += 1
        LOGGER.info("%s[%s] VALID %s (raw_id=%s): actual=%.6f predicted=%.6f error=%+.6f",
                    topic, partition, event_id, raw_id, event["actual_score"],
                    predicted_score, event["actual_score"] - predicted_score)
        return True

    except Exception:  # noqa: BLE001 - the consumer must survive ANY per-message failure
        db.rollback()
        COUNTERS["failed"] += 1
        LOGGER.exception("unhandled failure processing raw_id=%s - offset NOT committed", raw_id)
        return False


# ---------------------------------------------------------------------------
def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    config = load_config(parse_args())
    model = load_model(config["model_path"])
    db = connect_mysql(config["mysql"])
    consumer = build_consumer(config)
    consumer.subscribe([config["topic"]])
    LOGGER.info("subscribed: topic=%s group=%s bootstrap=%s",
                config["topic"], config["group"], config["bootstrap"])

    deadline = time.monotonic() + config["timeout"] if config["timeout"] else None
    try:
        while True:
            if config["max_messages"] is not None and COUNTERS["consumed"] >= config["max_messages"]:
                break
            message = consumer.poll(1.0)
            if message is None:
                if deadline is not None and time.monotonic() >= deadline:
                    LOGGER.info("idle timeout (%.0fs) reached - stopping", config["timeout"])
                    break
                continue
            if deadline is not None:
                deadline = time.monotonic() + config["timeout"]  # idle timer resets on activity

            # broker-level errors first: not counted as consumed, NOT committed
            if message.error() is not None:
                LOGGER.warning("message-level error on %s[%s] - skipped without commit: %s",
                               message.topic(), message.partition(), message.error())
                continue

            COUNTERS["consumed"] += 1
            try:
                committed = process_message(db, model, message)
            except Exception:  # noqa: BLE001 - process_message never raises
                LOGGER.exception("process_message failed unexpectedly - offset NOT committed")
                committed = False
            if committed:  # at-least-once: commit the offset ONLY after the DB commit
                consumer.commit(message=message, asynchronous=False)
    except KeyboardInterrupt:
        LOGGER.info("KeyboardInterrupt received - shutting down cleanly")
    finally:
        consumer.close()
        db.close()

    processed = {name: count for name, count in COUNTERS.items() if count}
    LOGGER.info("session summary: %s", ", ".join(f"{name}={count}" for name, count in processed.items()))
    return 0 if not COUNTERS["failed"] else 1


if __name__ == "__main__":
    sys.exit(main())
