-- =============================================================================
-- Workshop 3 - Streaming happiness pipeline - MySQL 8 schema (Part C, Step 7)
--
-- * Engine/charset: InnoDB + utf8mb4 (utf8mb4_unicode_ci) everywhere.
-- * Fully idempotent: every statement is CREATE TABLE IF NOT EXISTS, so this
--   file can be re-run at any time without touching existing data.
-- * Tables:
--     raw_happiness_events  - EVERY Kafka message exactly as received, BEFORE
--                             validation (raw_payload keeps the exact string,
--                             LONGTEXT so even malformed JSON is stored; a JSON
--                             column would reject it) + its processing status.
--     dim_country           - one row per country name (natural key).
--     dim_date              - one row per calendar date of event_time.
--     fact_predictions      - one row per validated event: model prediction and
--                             links to both dimensions and the raw event.
-- * Deduplication contract:
--     - raw_happiness_events accepts duplicates by design: same events may be
--       re-sent by re-running the producer, and invalid events may lack
--       event_id. Its surrogate PK is `id`; event_id stays NULLABLE and is
--       only indexed (no UNIQUE). The (topic, partition, offset) triple is
--       unique per broker record.
--     - fact_predictions enforces uniqueness per event_id, so re-running the
--       producer never inflates the analytical KPIs: the loader upserts
--       (INSERT ... ON DUPLICATE KEY UPDATE) instead of inserting duplicates.
-- * Sign convention: prediction_error = actual_score - predicted_score
--   (+ when the model undershoots, - when it overshoots). KPIs can aggregate
--   signed errors (bias) or use ABS(prediction_error) (e.g. MAE).
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Raw layer: store first, validate later
-- processing_status lifecycle:
--   PENDING          -> written on arrival, before any validation
--   VALID            -> passed validation and got a prediction
--   INVALID_SCHEMA   -> missing fields / wrong types / not parseable
--   INVALID_VALUES   -> present but semantically bad (NaN/inf/negative/non-ISO)
--   PREDICTION_ERROR -> valid event but the model failed to score it
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_happiness_events (
    id                BIGINT       NOT NULL AUTO_INCREMENT,
    event_id          VARCHAR(64)  NULL COMMENT 'Contract event id (E001...). NULL for invalid events without one; NOT unique: re-runs may re-send the same id.',
    raw_payload       LONGTEXT     NOT NULL COMMENT 'Exact Kafka message string as received (UTF-8), unmodified - malformed JSON stays here too.',
    kafka_topic       VARCHAR(128) NOT NULL,
    kafka_partition   INT          NOT NULL,
    kafka_offset      BIGINT       NOT NULL,
    received_at       DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT 'Consumption instant (MySQL server clock: container time).',
    processing_status VARCHAR(32)  NOT NULL DEFAULT 'PENDING',
    error_message     TEXT         NULL COMMENT 'Validation/prediction failure detail; NULL when VALID.',
    PRIMARY KEY (id),
    UNIQUE KEY uq_kafka_coords (kafka_topic, kafka_partition, kafka_offset),
    CONSTRAINT chk_raw_status CHECK (processing_status IN
        ('PENDING', 'VALID', 'INVALID_SCHEMA', 'INVALID_VALUES', 'PREDICTION_ERROR')),
    INDEX idx_raw_event_id (event_id),
    INDEX idx_raw_status (processing_status)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;


-- -----------------------------------------------------------------------------
-- Dimension: countries (natural key: country_name, which is also the Kafka key)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_country (
    country_id   INT          NOT NULL AUTO_INCREMENT,
    country_name VARCHAR(128) NOT NULL COMMENT 'Cleaned country name, as in the event contract.',
    PRIMARY KEY (country_id),
    UNIQUE KEY uq_country_name (country_name)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;


-- -----------------------------------------------------------------------------
-- Dimension: dates, keyed by date_id = YYYYMMDD integer (e.g. 20261030).
-- 8-digit integer key chosen instead of AUTO_INCREMENT so the loader can derive
-- the id straight from event_time (deterministic, stable across re-runs) with
-- no extra lookups. Covers the emission instant; report_year lives on the fact.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_id     INT         NOT NULL COMMENT 'Surrogate key = full_date as the integer YYYYMMDD.',
    full_date   DATE        NOT NULL,
    `year`      SMALLINT    NOT NULL,
    `month`     TINYINT     NOT NULL,
    `day`       TINYINT     NOT NULL,
    day_of_week VARCHAR(16) NOT NULL COMMENT 'Weekday name, e.g. Thursday.',
    PRIMARY KEY (date_id),
    UNIQUE KEY uq_dim_date_full_date (full_date)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;


-- -----------------------------------------------------------------------------
-- Fact: validated events with their predictions (KPI layer for the dashboard)
-- event_time = emission instant of the event; prediction_timestamp = row write.
-- report_year = historical survey year of the record (2015-2019), kept next to
-- the emission date ("when it was streamed") for trend KPIs ("which report
-- wave it belongs to").
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id        BIGINT     NOT NULL AUTO_INCREMENT,
    raw_event_id         BIGINT     NOT NULL COMMENT 'raw_happiness_events.id of the row this prediction came from.',
    event_id             VARCHAR(64) NOT NULL COMMENT 'Dedup key: unique here by design (raw_happiness_events stays non-unique).',
    country_id           INT        NOT NULL,
    date_id              INT        NOT NULL,
    report_year          SMALLINT   NOT NULL COMMENT 'Historical survey year of the record (2015-2019).',
    actual_score         DOUBLE     NOT NULL,
    predicted_score      DOUBLE     NOT NULL,
    prediction_error     DOUBLE     NOT NULL COMMENT 'actual_score - predicted_score (signed; use ABS() for MAE-style KPIs).',
    event_time           DATETIME(3) NOT NULL COMMENT 'Emission instant stated inside the event JSON.',
    prediction_timestamp DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT 'Row insertion instant.',
    PRIMARY KEY (prediction_id),
    UNIQUE KEY uq_fact_event_id (event_id),
    CONSTRAINT fk_fact_raw  FOREIGN KEY (raw_event_id) REFERENCES raw_happiness_events (id),
    CONSTRAINT fk_fact_country FOREIGN KEY (country_id) REFERENCES dim_country (country_id),
    CONSTRAINT fk_fact_date    FOREIGN KEY (date_id)    REFERENCES dim_date (date_id),
    INDEX idx_fact_country (country_id),
    INDEX idx_fact_date (date_id),
    INDEX idx_fact_event_time (event_time),
    INDEX idx_fact_report_year (report_year)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
