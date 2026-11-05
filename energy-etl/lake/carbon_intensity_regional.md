# lake.carbon_intensity_regional — Regional Carbon Intensity and Generation Mix

## Description

Forecast carbon intensity (gCO₂/kWh) and generation mix (%) for each of the 14 GB distribution network operator (DNO) regions, plus England, Scotland, Wales and GB totals, for every half-hour period since May 2018. From the Carbon Intensity API. One row per period and region with typed timestamps. Use it to compare how clean electricity is in different parts of the country.

## Classification

Public. Derived from public open data; no personal data.

## Owner

Dan. See the [energy-etl repo](https://github.com/DanteLore/energy-etl) for the code.

## Source & references

Derived from [`incoming.carbon_intensity_regional`](../incoming/carbon_intensity_regional.md). Original source: [Carbon Intensity API](https://carbon-intensity.github.io/api-docs/), regional endpoint.

## License & usage restrictions

Creative Commons Attribution 4.0 (CC BY 4.0). Free to use with attribution to the Carbon Intensity API.

## Location

- S3: `s3://dantelore.data.lake/carbon_intensity_regional/`
- Glue: `lake.carbon_intensity_regional`
- Format: Parquet (ParquetHiveSerDe)
- Not partitioned (about 2.6 million rows, 56 MB — a full scan of a few columns reads well under that)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| period_start_ts | timestamp | No | Primary (with region_id); join key to Elexon lake tables | UTC start of the half-hour period |
| period_end_ts | timestamp | No |  | UTC end of the half-hour period |
| region_id | int | No | Primary (with period_start_ts) | Carbon Intensity API region ID: 1–14 are DNO regions, 15 England, 16 Scotland, 17 Wales, 18 GB |
| region_name | string | No |  | Region name (e.g. `South West England`) |
| dno_region | string | No |  | DNO name (e.g. `WPD South West`); same as `region_name` for the aggregates |
| is_aggregate | boolean | No |  | True for the England, Scotland, Wales and GB totals (IDs 15–18) |
| intensity_forecast | int | Yes |  | Forecast carbon intensity (gCO₂/kWh) |
| intensity_index | string | Yes |  | Qualitative band: `very low`, `low`, `moderate`, `high`, `very high` |
| pct_gas | double | Yes |  | Gas share of generation (%) |
| pct_coal | double | Yes |  | Coal share (%) |
| pct_imports | double | Yes |  | Net imports share (%) |
| pct_biomass | double | Yes |  | Biomass share (%) |
| pct_nuclear | double | Yes |  | Nuclear share (%) |
| pct_hydro | double | Yes |  | Hydro share (%) |
| pct_wind | double | Yes |  | Wind share (%), including embedded |
| pct_solar | double | Yes |  | Solar share (%), including embedded |
| pct_other | double | Yes |  | Other share (%) |

## Source ETL / code

[`github.com/DanteLore/energy-etl`](https://github.com/DanteLore/energy-etl), `energy_data_model/sql/carbon_intensity_regional.sql`. Lambda function `model_energy_data`.

SQL summary: one row per `(period, region_id)`. Where a period appears in more than one incoming file, keeps the most recently written; strings cast to `timestamp`; adds `is_aggregate`.

## Freshness & update cadence

Full rebuild every night at 07:00 UTC by `model_energy_data`: the S3 folder is deleted and the whole table rewritten from `incoming.carbon_intensity_regional`. Incoming is updated every 30 minutes, so the lake lags by up to a day. Queries at 07:00 UTC may briefly see an empty table.

**History from:** 2018-05-10.

## Running costs (estimated)

Estimated monthly cost in USD at AWS eu-west-1 list prices, from measurements taken 2026-11-05: the S3 size of this table's location, plus the rebuild's measured Athena scan and incoming object count. Excludes tax and the AWS free tier (which currently covers all Lambda usage). Method: [energy-etl README](../README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.058 GB, 30 objects | $0.001 |
| Lambda `model_energy_data` (share) | 30 rebuilds/month | $0.001 |
| Athena (rebuild) | 30 × 1,046 MB scanned of `incoming.carbon_intensity_regional` | $0.157 |
| S3 reads (rebuild) | 30 × 3,262 objects | $0.039 |
| CloudWatch Logs | Lambda log ingestion | $0 |
| **Total** |  | **$0.20** |

The S3 reads grow by about 1,440 objects a month, because the Lambda writes one incoming file per half-hour period.

Querying it: a full scan reads about 0.06 GB, roughly <$0.001 per query (Athena bills a 10 MB minimum).

## Known issues & caveats

- **Filter on `is_aggregate`.** Summing or averaging over all 18 regions double-counts: the England, Scotland, Wales and GB rows are totals of the DNO regions.
- Forecast only: the API has no regional actual intensity. For national actuals use [`lake.carbon_intensity_national`](carbon_intensity_national.md).
- Don't average DNO regions to get a national figure; regions differ in size. Use the GB row (`region_id = 18`) or [`lake.carbon_intensity_generation`](carbon_intensity_generation.md).
- Percentages may not sum to exactly 100 because of rounding in the source.
- DNO regions are network areas, not administrative geography.
- Gaps in the source, where the API has no data: 2018-07-13 01:30 to 2018-07-16 01:00, 2018-08-03 23:30 to 2018-08-05 23:00, 2021-12-26 15:00 to 2021-12-27 13:00, 2023-10-20 22:00 to 2023-10-22 14:30, 2024-06-11 23:00 to 2024-06-12 09:30, 2025-01-12 23:00 to 2025-01-13 07:00 and 2025-08-10 23:00 to 2025-08-11 02:30.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
