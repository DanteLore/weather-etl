# lake.eia_brent_crude — Brent Crude Daily Spot Price

## Description

Europe Brent crude oil spot price (FOB) in US dollars per barrel for each trading day, from the US Energy Information Administration. One row per trading day with a typed date.

## Classification

Public. Derived from public open data; no personal data.

## Owner

Dan. See the [energy-etl repo](https://github.com/DanteLore/energy-etl) for the code.

## Source & references

Derived from [`incoming.eia_brent_crude`](../incoming/eia_brent_crude.md). Original source: US EIA open data API (series RBRTE).

## License & usage restrictions

US government open data. No copyright restrictions. Free to use, share and adapt.

## Location

- S3: `s3://dantelore.data.lake/eia_brent_crude/`
- Glue: `lake.eia_brent_crude`
- Format: Parquet (ParquetHiveSerDe)
- Not partitioned (small table — a full scan is a few MB)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| price_date | date | No | Primary | Trading day the price applies to |
| price_usd_per_barrel | double | No |  | Brent spot price (USD/barrel) |

## Source ETL / code

[`github.com/DanteLore/energy-etl`](https://github.com/DanteLore/energy-etl), `energy_data_model/sql/eia_brent_crude.sql`. Lambda function `model_energy_data`.

SQL summary: one row per date, latest-written copy wins; date cast from string.

## Freshness & update cadence

Full rebuild every night at 07:00 UTC by `model_energy_data`: the S3 folder is deleted and the whole table rewritten from `incoming.eia_brent_crude`. Incoming is loaded on weekdays at 20:00 UTC. The rebuild takes about 30 seconds for all energy lake tables; queries at 07:00 UTC may briefly see an empty table.

## Running costs (estimated)

Estimated monthly cost in USD at AWS eu-west-1 list prices, from measurements taken 2026-11-05: the S3 size of this table's location, plus the last 30 days of Lambda metrics and Athena query history. Excludes tax and the AWS free tier (which currently covers all Lambda usage). Method: [energy-etl README](../README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.000 GB, 1 objects | <$0.001 |
| Lambda `model_energy_data` (0% share) | 30 rebuilds/month | <$0.001 |
| Athena (rebuild) | 30 × 0 MB scanned of `incoming.eia_brent_crude` | $0.002 |
| S3 reads (rebuild) | 30 × 83 objects | $0.001 |
| CloudWatch Logs | Lambda log ingestion | $0 |
| **Total** | | **$0.003** |

Querying it: a full scan reads about 0.00 GB, roughly <$0.001 per query (Athena bills a 10 MB minimum).

## Known issues & caveats

- Weekends and US public holidays are absent by design; there is no traded price on those days.
- Prices are USD. There is no GBP conversion in the lake yet.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
