# lake.elexon_market_index — Wholesale Electricity Market Index Price

## Description

GB wholesale electricity price (Market Index Data, MID) for every half-hour settlement period, as a single volume-weighted `market_price` plus the per-provider figures from APX and N2EX. One row per period. Use this when you want "the" short-term wholesale power price.

## Classification

Public. Derived from public open data; no personal data.

## Owner

Dan. See the [energy-etl repo](https://github.com/DanteLore/energy-etl) for the code.

## Source & references

Derived from [`incoming.elexon_market_index`](../incoming/elexon_market_index.md). Original source: Elexon BMRS Market Index Data.

## License & usage restrictions

Elexon open data licence. Free to use; attribution to Elexon preferred. No restrictions on internal use.

## Location

- S3: `s3://dantelore.data.lake/elexon_market_index/`
- Glue: `lake.elexon_market_index`
- Format: Parquet (ParquetHiveSerDe)
- Not partitioned (small table — a full scan is a few MB)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| settlement_date | date | No | Primary (with settlement_period) | Settlement date (UK local calendar day) |
| settlement_period | int | No | Primary (with settlement_date) | Half-hour period 1–48 from local (Europe/London) midnight; 46 on the spring clock change day, 50 in autumn |
| period_start_ts | timestamp | No | Unique; join key across Elexon lake tables | UTC start of the settlement period. Use this to join Elexon lake tables to each other |
| market_price | double | Yes |  | Volume-weighted price across providers that reported volume (£/MWh). Null if no provider reported volume |
| market_volume | double | No |  | Total traded volume across providers (MWh) |
| apx_price | double | Yes |  | APX (EPEX) market index price (£/MWh) |
| apx_volume | double | Yes |  | APX traded volume (MWh) |
| n2ex_price | double | Yes |  | N2EX (Nord Pool) market index price (£/MWh). Usually 0 with 0 volume — see caveats |
| n2ex_volume | double | Yes |  | N2EX traded volume (MWh) |

## Source ETL / code

[`github.com/DanteLore/energy-etl`](https://github.com/DanteLore/energy-etl), `energy_data_model/sql/elexon_market_index.sql`. Lambda function `model_energy_data`.

SQL summary: de-duplicates per `(settlement_date, settlement_period, data_provider)`, then pivots providers into columns. `market_price = sum(price × volume) / sum(volume)`.

## Freshness & update cadence

Full rebuild every night at 07:00 UTC by `model_energy_data`: the S3 folder is deleted and the whole table rewritten from `incoming.elexon_market_index`. Incoming is updated every 30 minutes. The rebuild takes about 30 seconds for all energy lake tables; queries at 07:00 UTC may briefly see an empty table.

## Running costs (estimated)

Estimated monthly cost in USD at AWS eu-west-1 list prices, from measurements taken 2026-11-05: the S3 size of this table's location, plus the last 30 days of Lambda metrics and Athena query history. Excludes tax and the AWS free tier (which currently covers all Lambda usage). Method: [energy-etl README](../README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.003 GB, 5 objects | <$0.001 |
| Lambda `model_energy_data` (5% share) | 30 rebuilds/month | <$0.001 |
| Athena (rebuild) | 30 × 73 MB scanned of `incoming.elexon_market_index` | $0.011 |
| S3 reads (rebuild) | 30 × 2,501 objects | $0.030 |
| CloudWatch Logs | Lambda log ingestion | $0 |
| **Total** | | **$0.041** |

Querying it: a full scan reads about 0.00 GB, roughly <$0.001 per query (Athena bills a 10 MB minimum).

## Known issues & caveats

- **N2EX is effectively absent:** 117,754 of its 118,081 rows report zero volume and a zero price, so `market_price` is in practice the APX price. Never take an unweighted mean of the provider prices.
- `market_price` is null for 355 periods where neither provider reported volume.
- A provider's columns are null where it sent no row for that period (288 periods for APX, 436 for N2EX).
- If Elexon adds a new provider, it is included in `market_price`/`market_volume` but gets no column of its own until the SQL is updated.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
