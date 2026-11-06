-- =============================================================================
-- Workshop 3 - Streaming happiness pipeline - KPI queries (Part C, Step 9)
--
-- Analytical layer for `happiness_db` (MySQL 8). Every query below is a plain,
-- self-contained SELECT: run the whole file at once, or copy ONE query into
-- Power BI / the mysql client. All of them read the star schema only:
--     fact_predictions  (facts: 1 row per validated event, model prediction)
--     dim_country       (one row per country name)
--     dim_date          (one row per emission date, key = YYYYMMDD)
--     raw_happiness_events (landing audit for the ingestion-quality query)
--
-- Sign convention (also documented in create_tables.sql):
--     prediction_error = actual_score - predicted_score
--     positive  -> the model UNDERSHOOTS the real score
--     negative  -> the model OVERSHOOTS the real score
--     MAE-type KPIs use ABS(prediction_error); the signed mean is the model
--     bias. RMSE weights big mistakes more than MAE does.
--
-- The dashboard does NOT read CSV files: connect Power BI to MySQL
-- (localhost:3306, database happiness_db) and build the 4 visuals on these
-- same aggregations (see dashboards/README.md for the DAX equivalents).
-- =============================================================================


-- -----------------------------------------------------------------------------
-- KPI 1. Average prediction error - global model quality
--         MAE (mean absolute error), RMSE (root mean squared error) and the
--         signed mean error (bias: on average, how far the model leans).
-- -----------------------------------------------------------------------------
SELECT
    COUNT(*)                                  AS n_predictions,
    ROUND(AVG(ABS(prediction_error)), 4)      AS mae_global,
    ROUND(SQRT(AVG(POWER(prediction_error, 2))), 4) AS rmse_global,
    ROUND(AVG(prediction_error), 4)           AS mean_signed_error,
    ROUND(MAX(ABS(prediction_error)), 4)      AS max_abs_error
FROM fact_predictions;


-- -----------------------------------------------------------------------------
-- KPI 1b. Error breakdown per report year (2015-2019 survey wave)
--          Shows whether the model degrades on a specific survey wave.
-- -----------------------------------------------------------------------------
SELECT
    report_year,
    COUNT(*)                                  AS n_predictions,
    ROUND(AVG(actual_score), 4)               AS avg_actual_score,
    ROUND(AVG(ABS(prediction_error)), 4)      AS mae,
    ROUND(SQRT(AVG(POWER(prediction_error, 2))), 4) AS rmse,
    ROUND(AVG(prediction_error), 4)           AS mean_signed_error
FROM fact_predictions
GROUP BY report_year
ORDER BY report_year;


-- -----------------------------------------------------------------------------
-- KPI 2. Predictions by country - where the model is right and where it fails
--         Full ranking (one row per country), ordered by prediction count.
-- -----------------------------------------------------------------------------
SELECT
    c.country_name,
    COUNT(*)                             AS n_predictions,
    ROUND(AVG(f.actual_score), 4)        AS avg_actual_score,
    ROUND(AVG(f.predicted_score), 4)     AS avg_predicted_score,
    ROUND(AVG(ABS(f.prediction_error)), 4)  AS mae,
    ROUND(AVG(f.prediction_error), 4)    AS mean_signed_error
FROM fact_predictions f
JOIN dim_country c ON c.country_id = f.country_id
GROUP BY c.country_name
ORDER BY n_predictions DESC, c.country_name;


-- -----------------------------------------------------------------------------
-- KPI 2b. Best predictions per country (lowest MAE, min. 2 predictions)
--          "Where does the model hit most".
-- -----------------------------------------------------------------------------
SELECT
    c.country_name,
    COUNT(*)                             AS n_predictions,
    ROUND(AVG(ABS(f.prediction_error)), 4)  AS mae
FROM fact_predictions f
JOIN dim_country c ON c.country_id = f.country_id
GROUP BY c.country_name
HAVING COUNT(*) >= 2
ORDER BY mae ASC, n_predictions DESC
LIMIT 10;


-- -----------------------------------------------------------------------------
-- KPI 2c. Worst predictions per country (highest MAE, min. 2 predictions)
--          "Where does the model fail most".
-- -----------------------------------------------------------------------------
SELECT
    c.country_name,
    COUNT(*)                             AS n_predictions,
    ROUND(AVG(ABS(f.prediction_error)), 4)  AS mae,
    ROUND(AVG(f.prediction_error), 4)    AS mean_signed_error
FROM fact_predictions f
JOIN dim_country c ON c.country_id = f.country_id
GROUP BY c.country_name
HAVING COUNT(*) >= 2
ORDER BY mae DESC, n_predictions DESC
LIMIT 10;


-- -----------------------------------------------------------------------------
-- KPI 3. Predicted vs actual (event-level detail, scatter-plot ready)
--         One row per prediction: real score, model score, signed error,
--         absolute error and percentage deviation vs the real score.
--         Copy AS IS into Power BI as a table or use it to feed the scatter.
-- -----------------------------------------------------------------------------
SELECT
    f.event_id,
    c.country_name,
    f.report_year,
    ROUND(f.actual_score, 4)                    AS actual_score,
    ROUND(f.predicted_score, 4)                 AS predicted_score,
    ROUND(f.prediction_error, 4)                AS signed_error,
    ROUND(ABS(f.prediction_error), 4)           AS abs_error,
    ROUND(ABS(f.prediction_error) / NULLIF(f.actual_score, 0) * 100, 2) AS deviation_pct
FROM fact_predictions f
JOIN dim_country c ON c.country_id = f.country_id
ORDER BY c.country_name, f.report_year, f.event_id;


-- -----------------------------------------------------------------------------
-- KPI 4. Trend over time - per report year (the analytical trend)
--         Evolution of the real vs predicted average score and of the mean
--         error as the survey waves advance (2015 -> 2019).
-- -----------------------------------------------------------------------------
SELECT
    f.report_year,
    COUNT(*)                          AS n_predictions,
    ROUND(AVG(f.actual_score), 4)     AS avg_actual_score,
    ROUND(AVG(f.predicted_score), 4)  AS avg_predicted_score,
    ROUND(AVG(f.prediction_error), 4) AS mean_signed_error,
    ROUND(AVG(ABS(f.prediction_error)), 4) AS mae
FROM fact_predictions f
GROUP BY f.report_year
ORDER BY f.report_year;


-- -----------------------------------------------------------------------------
-- KPI 4b. Trend over time - per emission instant (the operational trend)
--          How the streaming run itself behaved: predictions grouped by the
--          hour in which the events were emitted (event_time from the JSON,
--          joined through dim_date for the calendar attributes).
-- -----------------------------------------------------------------------------
SELECT
    DATE_FORMAT(f.event_time, '%Y-%m-%d %H:00') AS emission_hour,
    d.day_of_week,
    COUNT(*)                          AS n_predictions,
    ROUND(AVG(f.actual_score), 4)     AS avg_actual_score,
    ROUND(AVG(f.predicted_score), 4)  AS avg_predicted_score,
    ROUND(AVG(f.prediction_error), 4) AS mean_signed_error
FROM fact_predictions f
JOIN dim_date d ON d.date_id = f.date_id
GROUP BY emission_hour, d.day_of_week
ORDER BY emission_hour;


-- -----------------------------------------------------------------------------
-- EXTRA. Ingestion quality summary (raw layer audit)
--         How many Kafka messages arrived, in which status, and the overall
--         validity rate. This is the health check of the streaming layer;
--         invalid rows never reach fact_predictions.
-- -----------------------------------------------------------------------------
SELECT
    processing_status,
    COUNT(*)                                   AS n_messages,
    ROUND(COUNT(*) / (SELECT COUNT(*) FROM raw_happiness_events) * 100, 2) AS pct_of_total
FROM raw_happiness_events
GROUP BY processing_status
ORDER BY n_messages DESC;
