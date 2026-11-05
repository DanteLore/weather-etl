# lake.elexon_netbsad — Net Balancing Services Adjustment Data

## Description

Elexon NETBSAD price and volume adjustments applied to the System Buy and Sell Prices for each half-hour settlement period. One row per period with typed dates and a derived UTC period start, so it can be joined directly to the other Elexon lake tables.

## Classification

Public. Derived from public open data; no personal data.

## Owner

Dan. See the [energy-etl repo](https://github.com/DanteLore/energy-etl) for the code.

## Source & references

Derived from [`incoming.elexon_netbsad`](../incoming/elexon_netbsad.md). Original source: Elexon BMRS NETBSAD.

## License & usage restrictions

Elexon open data licence. Free to use; attribution to Elexon preferred. No restrictions on internal use.

## Location

- S3: `s3://dantelore.data.lake/elexon_netbsad/`
- Glue: `lake.elexon_netbsad`
- Format: Parquet (ParquetHiveSerDe)
- Not partitioned (small table — a full scan is a few MB)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| settlement_date | date | No | Primary (with settlement_period) | Settlement date (UK local calendar day) |
| settlement_period | int | No | Primary (with settlement_date) | Half-hour period 1–48 from local (Europe/London) midnight; 46 on the spring clock change day, 50 in autumn |
| period_start_ts | timestamp | No | Unique; join key across Elexon lake tables | UTC start of the settlement period. Use this to join Elexon lake tables to each other |
| net_buy_price_cost_adjustment_energy | double | Yes |  | Energy cost adjustment applied to the buy price (£/MWh) |
| net_buy_price_volume_adjustment_energy | double | Yes |  | Energy volume adjustment applied to the buy price (MWh) |
| net_buy_price_volume_adjustment_system | double | Yes |  | System volume adjustment applied to the buy price (MWh) |
| buy_price_price_adjustment | double | Yes |  | Price adjustment to the System Buy Price (£/MWh) |
| net_sell_price_cost_adjustment_energy | double | Yes |  | Energy cost adjustment applied to the sell price (£/MWh) |
| net_sell_price_volume_adjustment_energy | double | Yes |  | Energy volume adjustment applied to the sell price (MWh) |
| net_sell_price_volume_adjustment_system | double | Yes |  | System volume adjustment applied to the sell price (MWh) |
| sell_price_price_adjustment | double | Yes |  | Price adjustment to the System Sell Price (£/MWh) |

## Source ETL / code

[`github.com/DanteLore/energy-etl`](https://github.com/DanteLore/energy-etl), `energy_data_model/sql/elexon_netbsad.sql`. Lambda function `model_energy_data`.

SQL summary: one row per `(settlement_date, settlement_period)`, latest-written copy wins. The source has no start time, so `period_start_ts` is derived from local midnight plus 30 minutes per period. That derivation was checked against every row of `elexon_system_prices` (119,986 periods, including clock changes) with no mismatches.

## Freshness & update cadence

Full rebuild every night at 07:00 UTC by `model_energy_data`: the S3 folder is deleted and the whole table rewritten from `incoming.elexon_netbsad`. Incoming is loaded daily at 06:00 UTC with the previous day's data. The rebuild takes about 30 seconds for all energy lake tables; queries at 07:00 UTC may briefly see an empty table.

## Known issues & caveats

- Values are zero for most periods; non-zero only when balancing service costs apply.
- In incoming, the `day` partition is one day behind the `settlement_date` it holds. Irrelevant here, as the lake keys on `settlement_date`.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
