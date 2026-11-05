# lake.carbon_intensity_generation — National Generation Mix

## Description

GB national generation mix as a percentage share by fuel for every half-hour period since May 2018, from the Carbon Intensity API. Unlike Elexon FUELHH, the wind and solar shares include estimates of embedded (behind-the-meter) generation such as rooftop solar. One row per half-hour period with typed timestamps.

## Classification

Public. Derived from public open data; no personal data.

## Owner

Dan. See the [energy-etl repo](https://github.com/DanteLore/energy-etl) for the code.

## Source & references

Derived from [`incoming.carbon_intensity_generation`](../incoming/carbon_intensity_generation.md). Original source: [Carbon Intensity API](https://carbon-intensity.github.io/api-docs/), generation endpoint.

## License & usage restrictions

Creative Commons Attribution 4.0 (CC BY 4.0). Free to use with attribution to the Carbon Intensity API.

## Location

- S3: `s3://dantelore.data.lake/carbon_intensity_generation/`
- Glue: `lake.carbon_intensity_generation`
- Format: Parquet (ParquetHiveSerDe)
- Not partitioned (small table — a full scan is a few MB)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| period_start_ts | timestamp | No | Primary; join key to Elexon lake tables | UTC start of the half-hour period |
| period_end_ts | timestamp | No |  | UTC end of the half-hour period |
| pct_gas | double | Yes |  | Gas share of generation (%) |
| pct_coal | double | Yes |  | Coal share (%) |
| pct_wind | double | Yes |  | Wind share (%), including embedded |
| pct_solar | double | Yes |  | Solar share (%), including embedded |
| pct_nuclear | double | Yes |  | Nuclear share (%) |
| pct_hydro | double | Yes |  | Hydro share (%) |
| pct_biomass | double | Yes |  | Biomass share (%) |
| pct_imports | double | Yes |  | Net imports share (%), all interconnectors combined |
| pct_other | double | Yes |  | Other share (%) |

## Source ETL / code

[`github.com/DanteLore/energy-etl`](https://github.com/DanteLore/energy-etl), `energy_data_model/sql/carbon_intensity_generation.sql`. Lambda function `model_energy_data`.

SQL summary: one row per period. Where a period appears in more than one incoming file, keeps the most recently written; strings cast to `timestamp`.

## Freshness & update cadence

Full rebuild every night at 07:00 UTC by `model_energy_data`: the S3 folder is deleted and the whole table rewritten from `incoming.carbon_intensity_generation`. Incoming is updated every 30 minutes, so the lake lags by up to a day. Queries at 07:00 UTC may briefly see an empty table.

**History from:** 2018-05-10 23:30.

## Running costs (estimated)

Estimated monthly cost in USD at AWS eu-west-1 list prices, from measurements taken 2026-11-05: the S3 size of this table's location, plus the rebuild's measured Athena scan and incoming object count. Excludes tax and the AWS free tier (which currently covers all Lambda usage). Method: [energy-etl README](../README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.004 GB, 16 objects | <$0.001 |
| Lambda `model_energy_data` (share) | 30 rebuilds/month | <$0.001 |
| Athena (rebuild) | 30 × 37 MB scanned of `incoming.carbon_intensity_generation` | $0.006 |
| S3 reads (rebuild) | 30 × 12,546 objects | $0.151 |
| CloudWatch Logs | Lambda log ingestion | $0 |
| **Total** |  | **$0.16** |

The S3 reads grow by about 1,440 objects a month, because the Lambda writes one incoming file per half-hour period.

Querying it: a full scan reads about 0.004 GB, roughly <$0.001 per query (Athena bills a 10 MB minimum).

## Known issues & caveats

- Shares, not MW. For absolute MW by fuel and per-interconnector detail use [`lake.elexon_fuelhh`](elexon_fuelhh.md), which is transmission-metered only, so its wind and solar figures are lower. The two are not directly comparable.
- Percentages may not sum to exactly 100 because of rounding in the source.
- Gaps in the source, where the API has no data: 2018-07-13 01:30 to 2018-07-16 01:00, 2018-08-03 23:30 to 2018-08-05 23:00, 2021-12-26 15:00 to 2021-12-27 13:00, 2023-10-20 22:00 to 2023-10-22 14:30, 2024-06-11 23:00 to 2024-06-12 09:30, 2025-01-12 23:00 to 2025-01-13 07:00 and 2025-08-10 23:00 to 2025-08-11 02:30.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
