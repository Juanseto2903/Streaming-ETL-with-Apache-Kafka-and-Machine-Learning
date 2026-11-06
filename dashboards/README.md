# Power BI Dashboard — Streaming Happiness Pipeline

This folder holds the Power BI deliverable: the report file and its reference
screenshots. The dashboard reads **only the MySQL database** (`happiness_db`,
localhost:3306) — never CSV files — so every visual always reflects what the
streaming pipeline stored.

```
dashboards/
├── README.md                                  <- this guide
├── happiness_dashboard.pbix                   <- Power BI report (save yours here)
├── diagrams/                                  <- README figures
│   ├── architecture.png                       (pipeline flow diagram)
│   ├── star_schema.png                        (star schema, exported from dbdiagram.io)
│   ├── eda_scale_shifts.png                   (per-year factor scale evidence)
│   ├── correlation_matrix.png                 (7x7 Pearson matrix)
│   └── model_diagnostics.png                  (predicted-vs-actual + residuals)
└── screenshots/                               <- 4 KPI reference views + your captures
    ├── 01_kpi_cards.png                       (reference preview generated from MySQL)
    ├── 02_scatter_actual_vs_predicted.png
    ├── 03_mae_by_country.png
    └── 04_trend_by_year.png
```

The `screenshots/*.png` included here are **reference previews** rendered straight
from `fact_predictions` with matplotlib (same aggregations as `sql/kpis.sql`) so the
expected result of every visual is documented even before building the .pbix.
After building the report, add the real Power BI captures with the same naming
(`0X_..._powerbi.png`) and save the report file as `happiness_dashboard.pbix`.

---

## 1. Connect Power BI Desktop to MySQL (step by step)

**Prerequisite (one time).** Power BI Desktop's MySQL connector needs the Oracle
MySQL Connector/NET (or MySQL ODBC driver). Install "MySQL Connector/NET"
(mysql-connector-net-x.x.x.msi) from Oracle's site, then restart Power BI.

1. Start the infrastructure and load data (from the repo root):
   ```powershell
   docker compose up -d                # wait for kafka + mysql healthy
   cmd /c "docker compose exec -T mysql mysql -uhappiness_user -pchange_me happiness_db < sql/create_tables.sql"   # idempotent
   # ... stream the events (producer + consumer), see README.md execution guide ...
   ```
2. Open **Power BI Desktop** → **Get Data** → **MySQL database**.
3. Connection dialog:
   * **Server:** `localhost:3306`
   * **Database:** `happiness_db`
4. Authentication → **Database** credentials:
   * **User name:** `happiness_user`
   * **Password:** `change_me` (the value of `MYSQL_PASSWORD` in your `.env`)
5. In the Navigator select ONLY these tables (do **not** import CSVs):
   * `fact_predictions` (fact)
   * `dim_country`, `dim_date` (dimensions)
   Optionally also `raw_happiness_events` if you want an ingestion-quality page.
6. Click **Load** (no transformations needed: the consumer already wrote clean,
   typed, deduplicated data).

> Data refresh: after re-running the streaming pipeline, just click **Refresh**
> in Power BI — it re-queries MySQL and every visual updates.

---

## 2. Star schema model (Model view)

Power BI usually auto-detects the relationships, but verify they look exactly
like this (single filter direction, dimension → fact):

| From (dimension)     | To (fact)              | Cardinality        | Cross-filter |
|----------------------|------------------------|--------------------|--------------|
| `dim_country[country_id]` | `fact_predictions[country_id]` | 1 : * (one-to-many) | Single |
| `dim_date[date_id]`       | `fact_predictions[date_id]`    | 1 : * (one-to-many) | Single |

```
dim_country ──1:*──► fact_predictions ◄──1:*── dim_date
   country_name        actual / predicted      calendar date of emission
                       prediction_error
                       report_year
```

Notes:
* `dim_date` is keyed by `date_id` = the emission date as `YYYYMMDD` (it carries
  `full_date`, `year`, `month`, `day`, `day_of_week`); the *survey* year lives on
  the fact as `fact_predictions[report_year]` (2015-2019) and drives the trend KPI.
* The fact is already deduplicated **per event** (`event_id` is UNIQUE), so
  `COUNTROWS(fact_predictions)` is the exact number of predictions — re-running
  the pipeline never inflates it.
* Do not create relationships from CSVs or copy tables manually.

---

## 3. Measures (DAX)

Create these measures in a dedicated measures table (Home → New Measure).
The auxiliary column first, then the measures:

**Calculated column** (on `fact_predictions`):
```dax
prediction_error_abs = ABS(fact_predictions[prediction_error])
```

**Measures:**
```dax
MAE = AVERAGE(fact_predictions[prediction_error_abs])

RMSE = SQRT(
    AVERAGEX(
        fact_predictions,
        POWER(fact_predictions[prediction_error], 2)
    )
)

Mean Signed Error (Bias) = AVERAGE(fact_predictions[prediction_error])

Predictions Count = COUNTROWS(fact_predictions)

Avg Actual Score = AVERAGE(fact_predictions[actual_score])

Avg Predicted Score = AVERAGE(fact_predictions[predicted_score])
```

Suggested visual semantics: MAE on a card with 2 decimals (the pipeline target
value is **0.43**); Bias near **-0.05** means the model leans slightly toward
overshooting. Per-year MAE (0.36 - 0.50) and per-country MAE (best ~0.06,
worst ~1.5) come from the same measures sliced by `report_year` / `country_name`.

---

## 4. The 4 required visuals (one report page)

| # | Visual | Fields | Setup |
|---|--------|--------|-------|
| 1 | **KPI cards** | 4 Card visuals: `MAE`, `RMSE` (or `Predictions Count`), `Mean Signed Error`, `Predictions Count` | One card per measure; conditional title "Global model quality". Expected: MAE 0.43, RMSE 0.56, Bias -0.05, Count 235 |
| 2 | **Scatter: predicted vs actual** | X = `actual_score`, Y = `predicted_score`, Details = `dim_country[country_name]` | Add a constant line Y = `Avg Actual Score` or use the "y = x" ideal: points hugging the diagonal = good predictions; catches outliers like Rwanda or Syria |
| 3 | **Bar / Map by country** | Axis = `dim_country[country_name]`, Value = `MAE` (and `Predictions Count` as second value or tooltip) | Sort descending by MAE (Bar chart) or use a Filled map on country names. Shows where the model hits most and fails most |
| 4 | **Trend over time** | X = `report_year` (or `dim_date[full_date]`), Values = `Avg Actual Score`, `Avg Predicted Score`, second axis `MAE` | Line chart per report year (2015→2019); the survey-wave trend the dashboard is judged on |

Optional extra page — **Ingestion quality**: table of
`raw_happiness_events[processing_status]` with `COUNTROWS` to show
`VALID` ~96% vs `INVALID_SCHEMA` / `INVALID_VALUES` (the resilience proof).

---

## 5. Oral presentation quick guide (30% of the grade)

Structure the demo around **the pipeline, not the model accuracy**:

1. **Architecture in one picture** (~2 min): raw CSVs → clean/harmonize (781 rows)
   → sklearn Pipeline → `model.pkl` → Kafka producer (JSON events, key = country)
   → consumer (store raw FIRST, validate, predict, upsert star schema) →
   MySQL 8 → Power BI. Emphasize at-least-once offsets: a Kafka offset is only
   committed AFTER the database commit, so nothing is lost and replays do not
   duplicate KPIs.
2. **Why OLS** (~2 min): the target is a continuous 0-10 index; a multiple linear
   regression with 6 interpretable socioeconomic factors is the right *baseline*
   (not a "too simple" shortcut) — standardized coefficients give a readable
   story (GDP and life expectancy dominate). Train vs test gap is tiny
   (MAE 0.4225 → 0.4321, R² 0.77 → 0.75): no overfitting, no tuning tricks.
3. **Resilience demo** (~2 min): show `raw_happiness_events` with its status
   column — 245 messages = 235 `VALID` + 10 deliberately broken ones (malformed
   JSON, `null` field, negative value, NaN, wrong types, missing Kafka key) that
   were stored, correctly flagged (`INVALID_SCHEMA` / `INVALID_VALUES`) and never
   allowed into `fact_predictions`. The consumer survived every one of them.
4. **KPIs and honesty about the numbers** (~2 min): MAE 0.4321 in the dashboard
   EXACTLY equals the offline test MAE (same rows, streamed through Kafka) —
   end-to-end numeric agreement, not a cherry-picked metric. Point at the worst
   country (Rwanda, systematic overshoot ~1.5) and the per-year view to show
   where the model struggles and why (factor-scale breaks between survey waves).
5. **Reproducibility close**: `docker compose up -d`, run the three scripts,
   open the .pbix → the same numbers appear again; idempotent schema + upserts
   make every re-run land on the same 235 predictions.
