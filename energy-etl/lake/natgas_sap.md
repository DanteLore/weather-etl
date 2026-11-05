# lake.natgas_sap — Gas System Average Price by Gas Day

## Description

National Gas System Average Price (SAP), the GB reference wholesale gas price, in pence per kWh for each gas day (06:00–06:00 UTC). One row per gas day with typed dates.

## Classification

Public. Derived from public open data; no personal data.

## Owner

Dan. See the [energy-etl repo](https://github.com/DanteLore/energy-etl) for the code.

## Source & references

Derived from [`incoming.natgas_sap`](../incoming/natgas_sap.md). Original source: National Gas Transmission, publication PUBOB603.

## License & usage restrictions

Open data. No restrictions on internal use. Attribution to National Gas Transmission if republished.

## Location

- S3: `s3://dantelore.data.lake/natgas_sap/`
- Glue: `lake.natgas_sap`
- Format: Parquet (ParquetHiveSerDe)
- Not partitioned (small table — a full scan is a few MB)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| gas_day | date | No | Primary | Gas day, dated by its start (the gas day runs 06:00–06:00 UTC) |
| sap_p_per_kwh | double | No |  | System Average Price (p/kWh) |
| published_ts | timestamp | No |  | UTC time the value became applicable (usually around 11:40 the day after the gas day) |
| quality_indicator | string | No |  | Quality code from National Gas; see caveats |
| is_substituted | boolean | No |  | True if National Gas substituted the value |

## Source ETL / code

[`github.com/DanteLore/energy-etl`](https://github.com/DanteLore/energy-etl), `energy_data_model/sql/natgas_sap.sql`. Lambda function `model_energy_data`.

SQL summary: one row per gas day, keeping the latest `applicable_at`; strings cast to `date`/`timestamp`.

## Freshness & update cadence

Full rebuild every night at 07:00 UTC by `model_energy_data`: the S3 folder is deleted and the whole table rewritten from `incoming.natgas_sap`. Incoming is loaded daily at 13:00 UTC, so the newest gas day appears in the lake the following morning. The rebuild takes about 30 seconds for all energy lake tables; queries at 07:00 UTC may briefly see an empty table.

## Known issues & caveats

- `quality_indicator` is `L` on 2,054 of 2,060 days and `A` on 6. Its meaning is unconfirmed — see the incoming entry.
- History starts 2021-03-15: the API only exposes a rolling window of about five years.
- To compare with electricity prices, note the units: p/kWh here, £/MWh in the Elexon tables (1 p/kWh = £10/MWh).

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
