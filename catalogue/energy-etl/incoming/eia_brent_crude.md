# incoming.eia_brent_crude

## Description

Europe Brent crude oil daily spot price in US dollars per barrel (USD/bbl), sourced from the US Energy Information Administration (EIA) REST API (series RBRTE). Covers weekdays only — weekends and US public holidays are absent by design (no trading). Use this as a commodity price reference for energy market analysis or to correlate oil prices with gas and electricity prices.

## Classification

Public. EIA data is published as US government open data with no restrictions.

## Owner

Dan (energy-etl). Query via `incoming.eia_brent_crude` in Athena.

## Source & references

- US EIA REST API v2: https://api.eia.gov/v2/
- Series: `RBRTE` (Europe Brent Spot Price FOB).
- Documentation: https://www.eia.gov/opendata/
- API key required (stored in `api_keys.py`, not in this repo).

## License & usage restrictions

US government open data. No copyright restrictions. Free to use, share and adapt.

## Location

- S3: `s3://dantelore.data.incoming/eia_brent_crude/`
- Glue / Athena: `incoming.eia_brent_crude`
- Format: NDJSON (JsonSerDe), partitioned by year/month (no day partition)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| date | string | No | Primary | Price date (YYYY-MM-DD). Weekdays only — weekend and US public holiday dates are absent |
| price_usd_per_barrel | double | No | | Brent crude spot price in US dollars per barrel (USD/bbl) |
| year | string | No | Partition | Unpadded year |
| month | string | No | Partition | Unpadded month |

## Source ETL / code

`energy-etl` repo, `eia_brent_crude_etl/`. Lambda runs daily at 20:00 UTC — after US market close and after the EIA typically publishes the day's price.

## Freshness & update cadence

Updated daily at 20:00 UTC on weekdays. The EIA publishes the previous business day's price; the day's price itself is published after market close (typically ~17:00 US Eastern time, so ~22:00 UTC). Weekend and holiday dates are structurally absent.

## Running costs (estimated)

Estimated monthly cost in USD at AWS eu-west-1 list prices, from measurements taken 2026-11-05: the S3 size of this table's location, plus the last 30 days of Lambda metrics and Athena query history. Excludes tax and the AWS free tier (which currently covers all Lambda usage). Method: [energy-etl README](../README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.000 GB, 83 objects | <$0.001 |
| Lambda `load_eia_brent_crude_data` | 34 runs × 15.7 s × 128 MB | $0.001 |
| S3 writes | ~34 files + 54 partition-DDL result files | $0.001 |
| Athena (loader) | 54 `ADD PARTITION` statements — DDL is free | $0 |
| CloudWatch Logs | Lambda log ingestion | <$0.001 |
| **Total** | | **$0.002** |

Querying it: a full scan reads about 0.00 GB, roughly <$0.001 per query (Athena bills a 10 MB minimum).

## Known issues & caveats

- **Weekends and US public holidays are absent.** This is not a gap — there is no traded price on those days. Do not interpret missing dates as pipeline failures. Use `CROSS JOIN` with a calendar table and `COALESCE` or forward-fill if you need a continuous daily series.
- **Price is in USD, not GBP.** Apply an FX rate if comparing to UK market prices denominated in pounds.
- **Partitioned by year/month only** (no day partition). Athena queries filtering by specific dates scan the full month partition.
- Partition columns are unpadded strings.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active. Modelled into [`lake.eia_brent_crude`](../lake/eia_brent_crude.md) — use that for analysis.
