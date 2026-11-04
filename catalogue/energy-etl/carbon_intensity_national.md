# carbon_intensity_national

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
| period_from | string | No | | ISO 8601 UTC start of the half-hour period (e.g. `2024-03-15T12:00Z`) |
| period_to | string | No | | ISO 8601 UTC end of the half-hour period |
| intensity_forecast | int | No | | Forecast carbon intensity in gCO₂/kWh, published at the start of the period |
| intensity_actual | int | Yes | | Actual carbon intensity in gCO₂/kWh; null until published, typically a few hours after period end |
| intensity_index | string | No | | Qualitative label: `very low`, `low`, `moderate`, `high`, `very high` |
| year | string | No | Partition | Unpadded year (e.g. `2024`) |
| month | string | No | Partition | Unpadded month (e.g. `3`) |
| day | string | No | Partition | Unpadded day (e.g. `15`) |

## Source ETL / code

`energy-etl` repo, `carbon_intensity_etl/`. Lambda runs every 30 minutes fetching the current period. Bulk historical load via `bulk_load.py` in 13-day chunks (API maximum is 14 days per request).

## Freshness & update cadence

Written every 30 minutes by Lambda. Each run writes one record for the current period. `intensity_actual` is null at write time and is not backfilled — the bulk load is the only way to populate actuals historically.

**History from:** 2018-06-01 (national); regional data available from May 2018.

## Known issues & caveats

- `intensity_actual` will be null for recently-written records. It is populated by the API with a lag of several hours; this table does not backfill it automatically. Use the bulk load to pull historical actuals.
- Partition columns are unpadded strings. Use `DATE_PARSE(year || '-' || month || '-' || day, '%Y-%c-%e')` in Athena, not `MAKE_DATE`.
- Lambda and bulk load use different S3 key patterns (per-period filename vs. per-day filename). Both are readable via the same Glue table.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
