# weather-etl

Datasets produced by the [weather-etl](https://github.com/dantelore/weather-etl) pipeline. Covers UK and Ireland meteorological observations from two sources: the Met Office DataHub API (live hourly feed) and the CEDA MIDAS Open archive (quality-controlled historic snapshot).

## How data flows

```
Met Office DataHub API  ──►  incoming.weather  ──►  lake.weather
                                                ──►  lake.weather_monthly_site_summary

CEDA MIDAS Open (2022,  ──►  incoming.midas         (no lake equivalent — deprecated)
 one-off bulk load)

datahub_etl/sites.json  ──►  lake.weather_stations
```

## incoming — raw landing zone

Written directly from source with no transformation beyond JSON flattening. These tables preserve exactly what the API returned. Use them for debugging, auditing, or reprocessing. Do not use them for analysis — duplicates and known bad data are present.

- S3 bucket: `dantelore.data.incoming`
- Athena database: `incoming`
- Format: NDJSON (JsonSerDe)

| Dataset | Table | Description | Status |
|---|---|---|---|
| [weather](incoming/weather.md) | `incoming.weather` | Raw hourly Met Office observations from the DataHub API, written every hour | Active |
| [midas](incoming/midas.md) | `incoming.midas` | Historic CEDA MIDAS Open observations 1875–2020, loaded January 2022 | **Deprecated** |

## lake — modelled, production-ready

Rebuilt nightly at **06:00 UTC** by the `model_weather_data` Lambda. Each run deletes and rewrites the full dataset from `incoming`. Deduplicated, filtered for known bad data, and stored as Parquet. Use these tables for analysis.

- S3 bucket: `dantelore.data.lake`
- Athena database: `lake`
- Format: Parquet (except `weather_stations`, which is JSON)

| Dataset | Table | Description |
|---|---|---|
| [weather](lake/weather.md) | `lake.weather` | Deduplicated, cleaned hourly Met Office observations — the main analysis table |
| [weather_stations](lake/weather_stations.md) | `lake.weather_stations` | Metadata for the 137 UK/Ireland Met Office monitoring stations |
| [weather_monthly_site_summary](lake/weather_monthly_site_summary.md) | `lake.weather_monthly_site_summary` | Percentile-based monthly temperature summary by station |

## Key things to know

- **Prefer `lake` over `incoming`** for any analysis. `incoming.weather` contains duplicates by design; `lake.weather` does not.
- **`lake` tables are fully rebuilt nightly.** Queries running around 06:00 UTC may see partial results. The rebuild takes a few minutes.
- **MIDAS data is a frozen snapshot** (1875–2020, CEDA 202107 release). It will not be updated. Glue partitions are not registered — run `MSCK REPAIR TABLE incoming.midas` in Athena before querying.
- **`lake.weather` excludes Chivenor July 2022** readings below −5°C. The raw data is still in `incoming.weather`.
- **`incoming.weather` is written every hour**, not once a day. The DataHub API returns 48 hours of history per call, so a missed run doesn't cause a gap — the next run will cover it.
