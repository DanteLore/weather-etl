# incoming.carbon_intensity_regional

## Description

Carbon intensity and generation mix broken down by DNO (Distribution Network Operator) region, half-hourly, from the Carbon Intensity API. Covers 14 UK regions including Scotland, Wales, and English DNO zones. Includes forecast intensity, intensity index, and percentage generation share for each fuel type. Use this when you need regional variation in carbon intensity rather than the national average.

## Classification

Public. The Carbon Intensity API is openly licensed with no authentication or usage restrictions.

## Owner

Dan (energy-etl). Query via `incoming.carbon_intensity_regional` in Athena.

## Source & references

- Carbon Intensity API: https://api.carbonintensity.org.uk
- Documentation: https://carbon-intensity.github.io/api-docs/
- No authentication required. Regional endpoint: `/regional/intensity/{from}/{to}` (range), `/regional` (current).

## License & usage restrictions

Open data, Creative Commons. No restrictions on use.

## Location

- S3: `s3://dantelore.data.incoming/carbon_intensity/regional/`
- Glue / Athena: `incoming.carbon_intensity_regional`
- Format: NDJSON (JsonSerDe), partitioned by year/month/day

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| period_from | string | No | | ISO 8601 UTC start of the half-hour period |
| period_to | string | No | | ISO 8601 UTC end of the half-hour period |
| region_id | int | No | Primary (with period_from) | Carbon Intensity API region ID (1–17) |
| region_name | string | No | | Human-readable region name (e.g. `South West England`) |
| dno_region | string | No | | DNO short name (e.g. `South Western`) |
| intensity_forecast | int | No | | Forecast carbon intensity in gCO₂/kWh |
| intensity_index | string | No | | Qualitative label: `very low`, `low`, `moderate`, `high`, `very high` |
| pct_gas | double | Yes | | Gas generation share (%) |
| pct_coal | double | Yes | | Coal generation share (%) |
| pct_imports | double | Yes | | Net imports share (%) — all interconnectors combined |
| pct_biomass | double | Yes | | Biomass generation share (%) |
| pct_nuclear | double | Yes | | Nuclear generation share (%) |
| pct_hydro | double | Yes | | Hydro generation share (%) |
| pct_wind | double | Yes | | Wind generation share (%) |
| pct_solar | double | Yes | | Solar generation share (%) |
| pct_other | double | Yes | | Other generation share (%) |
| year | string | No | Partition | Unpadded year |
| month | string | No | Partition | Unpadded month |
| day | string | No | Partition | Unpadded day |

## Source ETL / code

`energy-etl` repo, `carbon_intensity_etl/`. Same Lambda and bulk load as `carbon_intensity_national`; both feeds are fetched in a single invocation.

## Freshness & update cadence

Written every 30 minutes. Each run writes one row per region (14 rows) for the current period.

## Running costs (estimated)

Estimated monthly cost in USD at AWS eu-west-1 list prices, from measurements taken 2026-11-05: the S3 size of this table's location, plus the last 30 days of Lambda metrics and Athena query history. Excludes tax and the AWS free tier (which currently covers all Lambda usage). Method: [energy-etl README](../README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.970 GB, 14,625 objects | $0.022 |
| Lambda `load_carbon_intensity_data` (⅓ share) | 1,471 runs × 9.0 s × 128 MB | $0.009 |
| S3 writes | ~490 files + 1,441 partition-DDL result files | $0.017 |
| Athena (loader) | 1,441 `ADD PARTITION` statements — DDL is free | $0 |
| Athena (website charts) | `dantelore-charts` queries this table directly (~4 refreshes/month); each scans ~1 GB of JSON | $0.039 |
| CloudWatch Logs | Lambda log ingestion | $0.001 |
| **Total** | | **$0.088** |

Querying it: a full scan reads about 0.97 GB, roughly $0.005 per query (Athena bills a 10 MB minimum).

## Known issues & caveats

- No `intensity_actual` field — regional data only provides forecast intensity, unlike national.
- Percentage fields may not sum to exactly 100 due to rounding in the source API.
- Partition columns are unpadded strings. Use `DATE_PARSE(year || '-' || month || '-' || day, '%Y-%c-%e')` in Athena.
- Regional boundaries follow DNO zones, not administrative geography. Joining to local authority or postcode data requires a separate lookup.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
