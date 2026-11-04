# elexon_market_index

## Description

Market Index Data (MID) — wholesale electricity prices from UK power exchanges, per settlement period. Each row is one price from one data provider (N2EX or APX/Nordpool), giving the volume-weighted average price and total traded volume for that provider in that period. Use this for wholesale electricity price analysis. Note this is the market index, not the balancing mechanism system price — see `incoming.elexon_system_prices` for SBP/SSP.

## Classification

Public. Elexon BMRS data is published under the Balancing and Settlement Code and is freely accessible.

## Owner

Dan (energy-etl). Query via `incoming.elexon_market_index` in Athena.

## Source & references

- Elexon BMRS API: https://developer.data.elexon.co.uk/
- Market Index Data is submitted by approved Market Index Data Providers (MIDPs).
- API key required (stored in `api_keys.py`, not in this repo).

## License & usage restrictions

Elexon open data licence. Free to use; attribution to Elexon preferred. No restrictions on internal use.

## Location

- S3: `s3://dantelore.data.incoming/elexon_wholesale_price/market_index/`
- Glue / Athena: `incoming.elexon_market_index`
- Format: NDJSON (JsonSerDe), partitioned by year/month/day

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| settlement_date | string | No | Primary (composite) | Settlement date (YYYY-MM-DD) |
| settlement_period | int | No | Primary (composite) | Half-hour period 1–48 |
| start_time | string | No | | ISO 8601 UTC start of the period |
| data_provider | string | No | Primary (composite) | Exchange/provider name (e.g. `N2EX`, `APX`) |
| price | double | Yes | | Volume-weighted average price in £/MWh |
| volume | double | Yes | | Total traded volume in MWh |
| year | string | No | Partition | Unpadded year |
| month | string | No | Partition | Unpadded month |
| day | string | No | Partition | Unpadded day |

## Source ETL / code

`energy-etl` repo, `elexon_wholesale_price_etl/`. Written by the same Lambda as `elexon_system_prices`; both are fetched per invocation.

## Freshness & update cadence

Updated every 30 minutes. Multiple rows per period (one per data provider).

## Known issues & caveats

- There are typically two rows per settlement period (one each from N2EX and APX). Aggregating across providers requires a weighted average using the `volume` field, not a simple mean of `price`.
- The set of data providers may change over time as Elexon approves or removes MIDPs.
- Recent periods may be absent if the exchange has not yet submitted data.
- Partition columns are unpadded strings.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
