# elexon_netbsad

## Description

Net Balancing Services Adjustment Data (NETBSAD) from Elexon — the adjustments applied to the System Buy and Sell Prices to account for costs incurred through balancing services (e.g. STOR, frequency response) that are separate from the main energy balancing. Each row covers one settlement period. These adjustments feed into the final cash-out price calculation. Use this if you need to reconcile system prices or understand the component breakdown of balancing costs.

## Classification

Public. Elexon BMRS data is published under the Balancing and Settlement Code and is freely accessible.

## Owner

Dan (energy-etl). Query via `incoming.elexon_netbsad` in Athena.

## Source & references

- Elexon BMRS API: https://developer.data.elexon.co.uk/
- Report: NETBSAD (Net Balancing Services Adjustment Data).
- API key required (stored in `api_keys.py`, not in this repo).

## License & usage restrictions

Elexon open data licence. Free to use; attribution to Elexon preferred. No restrictions on internal use.

## Location

- S3: `s3://dantelore.data.incoming/elexon_netbsad/`
- Glue / Athena: `incoming.elexon_netbsad`
- Format: NDJSON (JsonSerDe), partitioned by year/month/day

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| settlement_date | string | No | Primary (with settlement_period) | Settlement date (YYYY-MM-DD) |
| settlement_period | int | No | Primary (with settlement_date) | Half-hour period 1–48 |
| net_buy_price_cost_adjustment_energy | double | Yes | | Energy cost adjustment applied to the buy price (£/MWh) |
| net_buy_price_volume_adjustment_energy | double | Yes | | Energy volume adjustment applied to the buy price (MWh) |
| net_buy_price_volume_adjustment_system | double | Yes | | System volume adjustment applied to the buy price (MWh) |
| buy_price_price_adjustment | double | Yes | | Price adjustment to the System Buy Price (£/MWh) |
| net_sell_price_cost_adjustment_energy | double | Yes | | Energy cost adjustment applied to the sell price (£/MWh) |
| net_sell_price_volume_adjustment_energy | double | Yes | | Energy volume adjustment applied to the sell price (MWh) |
| net_sell_price_volume_adjustment_system | double | Yes | | System volume adjustment applied to the sell price (MWh) |
| sell_price_price_adjustment | double | Yes | | Price adjustment to the System Sell Price (£/MWh) |
| year | string | No | Partition | Unpadded year |
| month | string | No | Partition | Unpadded month |
| day | string | No | Partition | Unpadded day |

## Source ETL / code

`energy-etl` repo, `elexon_netbsad_etl/`. Lambda runs daily at 06:00 UTC, fetching the previous day's data.

## Freshness & update cadence

Updated daily at 06:00 UTC with the previous day's data.

**History from:** 2020-01-01. Typical volume: 48 rows/day (one per settlement period). Values are zero on most days; non-zero when specific balancing service costs are applied.

## Known issues & caveats

- These adjustments are the most opaque part of the cash-out calculation. The Elexon BSC documentation (Section T) is the authoritative reference for interpreting each field.
- Values can be positive or negative depending on whether balancing services added cost or credit to the system.
- Partition columns are unpadded strings.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
