# elexon_bod

## Description

Bid-Offer Data (BOD) from Elexon's Balancing Mechanism — the prices and MW levels at which Balancing Mechanism Units (BMUs) are willing to be instructed up or down. Each row is one pair of bid/offer levels for one BM unit in one settlement period. This is the raw offer book submitted by generators and large consumers before despatch instructions are issued. Use `incoming.elexon_boalf` to see which bids and offers were actually accepted.

## Classification

Public. Elexon BMRS data is published under the Balancing and Settlement Code and is freely accessible.

## Owner

Dan (energy-etl). Query via `incoming.elexon_bod` in Athena.

## Source & references

- Elexon BMRS API: https://developer.data.elexon.co.uk/
- Report: BOD (Bid-Offer Data).
- API key required (stored in `api_keys.py`, not in this repo).

## License & usage restrictions

Elexon open data licence. Free to use; attribution to Elexon preferred. No restrictions on internal use.

## Location

- S3: `s3://dantelore.data.incoming/elexon_bod/`
- Glue / Athena: `incoming.elexon_bod`
- Format: Parquet (Snappy), partitioned by year/month/day

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| settlement_date | string | No | Primary (composite) | Settlement date (YYYY-MM-DD) |
| settlement_period | int | No | Primary (composite) | Half-hour period 1–48 |
| bm_unit | string | No | Primary (composite) | BM unit ID (e.g. `T_DRAXX-1`) |
| national_grid_bm_unit | string | Yes | | National Grid's equivalent BM unit identifier |
| pair_id | int | No | Primary (composite) | Bid-offer pair index within the unit's stack |
| time_from | string | No | | ISO 8601 UTC start of the level-pair's validity window |
| time_to | string | No | | ISO 8601 UTC end of the level-pair's validity window |
| level_from | double | No | | MW output at the start of the pair window |
| level_to | double | No | | MW output at the end of the pair window |
| bid_price | double | Yes | | Price (£/MWh) the unit will accept to reduce output (negative = the unit pays to reduce) |
| offer_price | double | Yes | | Price (£/MWh) the unit requires to increase output |
| year | string | No | Partition | Unpadded year |
| month | string | No | Partition | Unpadded month |
| day | string | No | Partition | Unpadded day |

## Source ETL / code

`energy-etl` repo, `elexon_bod_etl/`. Lambda runs daily at 06:00 UTC, fetching the previous day's data.

## Freshness & update cadence

Updated daily at 06:00 UTC with the previous day's data. Approximately one day of lag.

**History from:** 2020-01-01. Typical volume: ~180,000 rows/day. Storage format is Parquet (Snappy) — ~900 KB/day vs ~50 MB if stored as NDJSON.

## Known issues & caveats

- A single BM unit may submit multiple bid-offer pairs (stacked levels). Joining to `elexon_boalf` on `(settlement_date, settlement_period, bm_unit, pair_id)` gives the accepted subset.
- `bid_price` can be negative — some units pay for the right to reduce output (e.g. must-run plant).
- BOD is submitted before the gate closure; units may revise submissions up to gate closure. Only the final submission is stored here.
- Partition columns are unpadded strings.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
