# lake.carbon_intensity_national — National Carbon Intensity

## Description

GB national carbon intensity of electricity in gCO₂/kWh for every half-hour period since 2018, from the Carbon Intensity API (National Energy System Operator with the University of Oxford and WWF): the forecast, the actual estimate and a qualitative index. One row per half-hour period with typed timestamps. The main table for "how clean was the grid at time X".

## Classification

Public. Derived from public open data; no personal data.

## Owner

Dan. See the [energy-etl repo](https://github.com/DanteLore/energy-etl) for the code.

## Source & references

Derived from [`incoming.carbon_intensity_national`](../incoming/carbon_intensity_national.md). Original source: [Carbon Intensity API](https://carbon-intensity.github.io/api-docs/).

## License & usage restrictions

Creative Commons Attribution 4.0 (CC BY 4.0). Free to use with attribution to the Carbon Intensity API.

## Location

- S3: `s3://dantelore.data.lake/carbon_intensity_national/`
- Glue: `lake.carbon_intensity_national`
- Format: Parquet (ParquetHiveSerDe)
- Not partitioned (small table — a full scan is a few MB)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| period_start_ts | timestamp | No | Primary; join key to Elexon lake tables | UTC start of the half-hour period. Matches `period_start_ts` in the Elexon lake tables |
| period_end_ts | timestamp | No |  | UTC end of the half-hour period (start + 30 minutes) |
| intensity_forecast | int | Yes | | Forecast carbon intensity (gCO₂/kWh) |
| intensity_actual | int | Yes | | Actual carbon intensity (gCO₂/kWh), estimated from metered generation after the period |
| intensity_index | string | Yes | | Qualitative band: `very low`, `low`, `moderate`, `high`, `very high` |

## Source ETL / code

[`github.com/DanteLore/energy-etl`](https://github.com/DanteLore/energy-etl), `energy_data_model/sql/carbon_intensity_national.sql`. Lambda function `model_energy_data`.

SQL summary: one row per period. Where a period appears in more than one incoming file (per-period Lambda files alongside bulk-load day files), keeps a row with an actual if there is one, then the most recently written; strings cast to `timestamp`.

## Freshness & update cadence

Full rebuild every night at 07:00 UTC by `model_energy_data`: the S3 folder is deleted and the whole table rewritten from `incoming.carbon_intensity_national`. Incoming is updated every 30 minutes, so the lake lags by up to a day. Queries at 07:00 UTC may briefly see an empty table.

**History from:** 2018-01-01 (plus the 2017-12-31 23:30 period).

## Running costs (estimated)

Estimated monthly cost in USD at AWS eu-west-1 list prices, from measurements taken 2026-11-05: the S3 size of this table's location, plus the rebuild's measured Athena scan and incoming object count. Excludes tax and the AWS free tier (which currently covers all Lambda usage). Method: [energy-etl README](../README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.003 GB, 16 objects | <$0.001 |
| Lambda `model_energy_data` (share) | 30 rebuilds/month | <$0.001 |
| Athena (rebuild) | 30 × 25 MB scanned of `incoming.carbon_intensity_national` | $0.004 |
| S3 reads (rebuild) | 30 × 3,262 objects | $0.039 |
| CloudWatch Logs | Lambda log ingestion | $0 |
| **Total** |  | **$0.04** |

The S3 reads grow by about 1,440 objects a month, because the Lambda writes one incoming file per half-hour period.

Querying it: a full scan reads about 0.003 GB, roughly <$0.001 per query (Athena bills a 10 MB minimum).

## Known issues & caveats

- `intensity_actual` is null for about 625 periods, all in 2018–2023, where the API has no actual either. Use `coalesce(intensity_actual, intensity_forecast)` for a continuous series.
- Gaps in the source, where the API has no data: 2021-12-26 15:00 to 2021-12-27 17:30, 2023-10-20 22:00 to 2023-10-22 19:00, 2024-06-11 23:00 to 2024-06-12 14:00 and three periods on 2021-04-19.
- Carbon intensity includes embedded wind and solar estimates, unlike Elexon FUELHH (transmission-metered only).

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
