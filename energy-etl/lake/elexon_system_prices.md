# lake.elexon_system_prices — System Buy/Sell Prices

## Description

Elexon System Buy Price and System Sell Price (the imbalance "cash-out" prices) with net imbalance volume and accepted bid/offer volumes, for every half-hour settlement period. One row per period, typed.

## Classification

Public. Derived from public open data; no personal data.

## Owner

Dan. See the [energy-etl repo](https://github.com/DanteLore/energy-etl) for the code.

## Source & references

Derived from [`incoming.elexon_system_prices`](../incoming/elexon_system_prices.md). Original source: Elexon BMRS system prices.

## License & usage restrictions

Elexon open data licence. Free to use; attribution to Elexon preferred. No restrictions on internal use.

## Location

- S3: `s3://dantelore.data.lake/elexon_system_prices/`
- Glue: `lake.elexon_system_prices`
- Format: Parquet (ParquetHiveSerDe)
- Not partitioned (small table — a full scan is a few MB)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| settlement_date | date | No | Primary (with settlement_period) | Settlement date (UK local calendar day) |
| settlement_period | int | No | Primary (with settlement_date) | Half-hour period 1–48 from local (Europe/London) midnight; 46 on the spring clock change day, 50 in autumn |
| period_start_ts | timestamp | No | Unique; join key across Elexon lake tables | UTC start of the settlement period. Use this to join Elexon lake tables to each other |
| system_sell_price | double | Yes |  | System Sell Price in £/MWh (paid to long parties) |
| system_buy_price | double | Yes |  | System Buy Price in £/MWh (charged to short parties) |
| net_imbalance_volume | double | Yes |  | Net imbalance volume in MWh (positive = system long) |
| price_derivation_code | string | Yes |  | Code indicating how prices were derived (e.g. `A`, `B`, `C`) |
| reserve_scarcity_price | double | Yes |  | Reserve scarcity price applied, if any (£/MWh) |
| total_accepted_offer_volume | double | Yes |  | Total volume accepted from offers (MWh) |
| total_accepted_bid_volume | double | Yes |  | Total volume accepted from bids (MWh) |
| replacement_price | double | Yes |  | Replacement price used in price derivation where applicable (£/MWh) |

## Source ETL / code

[`github.com/DanteLore/energy-etl`](https://github.com/DanteLore/energy-etl), `energy_data_model/sql/elexon_system_prices.sql`. Lambda function `model_energy_data`.

SQL summary: one row per `(settlement_date, settlement_period)`, latest-written copy wins; strings cast to `date`/`timestamp`.

## Freshness & update cadence

Full rebuild every night at 07:00 UTC by `model_energy_data`: the S3 folder is deleted and the whole table rewritten from `incoming.elexon_system_prices`. Incoming is updated every 30 minutes. The rebuild takes about 30 seconds for all energy lake tables; queries at 07:00 UTC may briefly see an empty table.

## Known issues & caveats

- Prices for recent periods may be preliminary. Elexon corrections are not backfilled into incoming, so they are not here either.
- `reserve_scarcity_price` is null for about a third of periods and `replacement_price` for about 85%: they are only published when they apply.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
