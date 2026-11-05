# incoming.carbon_intensity_national

## Description

National UK carbon intensity data, half-hourly, from the Carbon Intensity API (carbonintensity.org.uk). Covers forecast intensity, actual intensity (populated a few hours after the period ends), and a qualitative intensity index (very low / low / moderate / high / very high). The primary dataset for understanding how clean UK electricity generation is at any given time.

## Classification

Public. The Carbon Intensity API is openly licensed with no authentication or usage restrictions.

## Owner

Dan (energy-etl). Query via `incoming.carbon_intensity_national` in Athena.

## Source & references

- Carbon Intensity API: https://api.carbonintensity.org.uk
- Documentation: https://carbon-intensity.github.io/api-docs/
- No authentication required. National endpoint: `/intensity/{from}/{to}` (range), `/intensity` (current period).

## License & usage restrictions

Open data, Creative Commons. No restrictions on use.

## Location

- S3: `s3://dantelore.data.incoming/carbon_intensity/national/`
- Glue / Athena: `incoming.carbon_intensity_national`
- Format: NDJSON (JsonSerDe), partitioned by year/month/day

S3 key pattern (Lambda, per-period): `carbon_intensity/national/year={Y}/month={M}/day={D}/intensity-{YYYY-MM-DD-HHmm}.json`
S3 key pattern (bulk load, per-day): `carbon_intensity/national/year={Y}/month={M}/day={D}/intensity-{YYYY-MM-DD}.json`

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| period_from | string | No | | UTC start of the half-hour period as `YYYY-MM-DD HH:MM` (e.g. `2024-03-15 12:00`) |
| period_to | string | No | | UTC end of the half-hour period, same format |
| intensity_forecast | int | No | | Forecast carbon intensity in gCO₂/kWh, published at the start of the period |
| intensity_actual | int | Yes | | Actual carbon intensity in gCO₂/kWh; null until published, typically a few hours after period end |
| intensity_index | string | No | | Qualitative label: `very low`, `low`, `moderate`, `high`, `very high` |
| year | string | No | Partition | Unpadded year (e.g. `2024`) |
| month | string | No | Partition | Unpadded month (e.g. `3`) |
| day | string | No | Partition | Unpadded day (e.g. `15`) |

## Source ETL / code

`energy-etl` repo, `carbon_intensity_etl/`. Lambda runs every 30 minutes fetching the last 6 hours of finished periods. Bulk historical load via `bulk_load.py` in 13-day chunks (API maximum is 14 days per request).

## Freshness & update cadence

Written every 30 minutes by Lambda. Each run re-fetches the last 6 hours of finished periods and rewrites one file per period, so a run lost to an API outage is filled in by a later one, and values the API revises within 6 hours are picked up. Periods are only loaded once finished, so `intensity_actual` is populated.

**History from:** 2018-01-01 (national); regional from 2018-05-10.

## Running costs (estimated)

Estimated monthly cost in USD at AWS eu-west-1 list prices, from measurements taken 2026-11-05: the S3 size of this table's location, plus the last 30 days of Lambda metrics and Athena query history. Excludes tax and the AWS free tier (which currently covers all Lambda usage). Method: [energy-etl README](../README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.024 GB, 14,625 objects | $0.001 |
| Lambda `load_carbon_intensity_data` (⅓ share) | 1,471 runs × 9.0 s × 128 MB | $0.009 |
| S3 writes | ~490 files + 1,441 partition-DDL result files | $0.017 |
| Athena (loader) | 1,441 `ADD PARTITION` statements — DDL is free | $0 |
| CloudWatch Logs | Lambda log ingestion | $0.001 |
| **Total** | | **$0.027** |

Querying it: a full scan reads about 0.02 GB, roughly <$0.001 per query (Athena bills a 10 MB minimum).

## Known issues & caveats

- `intensity_actual` is the API's figure up to 6 hours after the period ends; any later revisions aren't picked up. Re-run the bulk load for a date range to refresh it.
- Partition columns are unpadded strings. Use `DATE_PARSE(year || '-' || month || '-' || day, '%Y-%c-%e')` in Athena, not `MAKE_DATE`.
- Lambda and bulk load use different S3 key patterns (per-period filename vs. per-day filename). Both are readable via the same Glue table. Each day has one or the other: where a bulk load covered a day that already had per-period files, the per-period files were deleted (2026-11-05). One period on 2021-04-19 appears twice within its file; [`lake.carbon_intensity_national`](../lake/carbon_intensity_national.md) de-duplicates.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active. Modelled into [`lake.carbon_intensity_national`](../lake/carbon_intensity_national.md) — use that for analysis.
