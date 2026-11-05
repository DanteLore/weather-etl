# incoming.elexon_system_prices

## Description

Elexon System Buy Price (SBP) and System Sell Price (SSP) per settlement period, derived from the Balancing Mechanism. These are the cash-out prices paid by parties that are out of balance — the SBP applies to those short of energy, the SSP to those long. Also includes net imbalance volume, total accepted bid/offer volumes, and the price derivation code indicating how the prices were calculated. The primary dataset for understanding balancing mechanism costs and imbalance pricing.

## Classification

Public. Elexon BMRS data is published under the Balancing and Settlement Code and is freely accessible.

## Owner

Dan (energy-etl). Query via `incoming.elexon_system_prices` in Athena.

## Source & references

- Elexon BMRS API: https://developer.data.elexon.co.uk/
- Report: System Prices (B1770 equivalent), settlement period resolution.
- API key required (stored in `api_keys.py`, not in this repo).

## License & usage restrictions

Elexon open data licence. Free to use; attribution to Elexon preferred. No restrictions on internal use.

## Location

- S3: `s3://dantelore.data.incoming/elexon_wholesale_price/system_prices/`
- Glue / Athena: `incoming.elexon_system_prices`
- Format: NDJSON (JsonSerDe), partitioned by year/month/day

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| settlement_date | string | No | Primary (with settlement_period) | Settlement date (YYYY-MM-DD) |
| settlement_period | int | No | Primary (with settlement_date) | Half-hour period 1–48 (period 1 = 00:00–00:30 UTC) |
| start_time | string | No | | ISO 8601 UTC start of the period |
| system_sell_price | double | Yes | | System Sell Price in £/MWh (paid to long parties) |
| system_buy_price | double | Yes | | System Buy Price in £/MWh (charged to short parties) |
| net_imbalance_volume | double | Yes | | Net imbalance volume in MWh (positive = system long) |
| price_derivation_code | string | Yes | | Code indicating how prices were derived (e.g. `A`, `B`, `C`) |
| reserve_scarcity_price | double | Yes | | Reserve scarcity price applied, if any (£/MWh) |
| total_accepted_offer_volume | double | Yes | | Total volume accepted from offers (MWh) |
| total_accepted_bid_volume | double | Yes | | Total volume accepted from bids (MWh) |
| replacement_price | double | Yes | | Replacement price used in price derivation where applicable (£/MWh) |
| year | string | No | Partition | Unpadded year |
| month | string | No | Partition | Unpadded month |
| day | string | No | Partition | Unpadded day |

## Source ETL / code

`energy-etl` repo, `elexon_wholesale_price_etl/`. Lambda runs every 30 minutes, fetching system prices alongside market index data.

## Freshness & update cadence

Updated every 30 minutes. Elexon publishes system prices after each settlement period; recent periods may be preliminary and subject to revision.

**History from:** 2020-01-01.

## Running costs (estimated)

Estimated monthly cost in USD at AWS eu-west-1 list prices, from measurements taken 2026-11-05: the S3 size of this table's location, plus the last 30 days of Lambda metrics and Athena query history. Excludes tax and the AWS free tier (which currently covers all Lambda usage). Method: [energy-etl README](../README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.044 GB, 2,501 objects | $0.001 |
| Lambda `load_elexon_wholesale_price_data` (½ share) | 1,440 runs × 11.3 s × 128 MB | $0.017 |
| S3 writes | ~720 files + 2,869 partition-DDL result files | $0.032 |
| Athena (loader) | 2,869 `ADD PARTITION` statements — DDL is free | $0 |
| CloudWatch Logs | Lambda log ingestion | $0.001 |
| **Total** | | **$0.051** |

Querying it: a full scan reads about 0.04 GB, roughly <$0.001 per query (Athena bills a 10 MB minimum).

## Known issues & caveats

- System prices for the most recent periods are preliminary. Final settled prices may differ. Elexon issues corrections; this table is not backfilled with corrections automatically.
- `price_derivation_code` values are documented in the Elexon BSC (Balancing and Settlement Code) — see Elexon's own documentation for the code meanings.
- On the rare occasion that system prices cannot be derived, fields may be null.
- Partition columns are unpadded strings.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active. Modelled into [`lake.elexon_system_prices`](../lake/elexon_system_prices.md) — use that for analysis.
