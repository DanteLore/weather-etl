# natgas_sap

## Description

National Gas System Average Price (SAP) — the volume-weighted average price of natural gas traded on the GB gas network for each gas day, in pence per kilowatt hour (p/kWh). Published daily by National Gas Transmission via their REST API (publication PUBOB603). The SAP is a widely-used reference price for gas imbalance settlement and short-term gas market analysis. Each gas day may have multiple records with different `quality_indicator` values as estimates are refined into actuals.

## Classification

Public. Published by National Gas Transmission as open data.

## Owner

Dan (energy-etl). Query via `incoming.natgas_sap` in Athena.

## Source & references

- National Gas Transmission Data Portal: https://data.nationalgas.com/
- Publication: PUBOB603 (System Average Price).
- No authentication required.

## License & usage restrictions

Open data. No restrictions on internal use. Attribution to National Gas Transmission if republished.

## Location

- S3: `s3://dantelore.data.incoming/natgas_sap/`
- Glue / Athena: `incoming.natgas_sap`
- Format: NDJSON (JsonSerDe), partitioned by year/month (no day partition)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| applicable_for | string | No | Primary (with applicable_at) | Gas day date the price applies to (YYYY-MM-DD) |
| applicable_at | string | No | Primary (with applicable_for) | UTC datetime when this value became applicable; use to get the latest revision per day |
| value | double | No | | System Average Price in pence per kilowatt hour (p/kWh) |
| quality_indicator | string | No | | `E`=Estimate, `A`=Actual, `C`=Calculated, `S`=Substituted, `L`=Late |
| substituted | string | No | | `Y` if the value has been substituted, `N` otherwise |
| create_date | string | No | | UTC datetime when this record was created/published by National Gas |
| year | string | No | Partition | Unpadded year |
| month | string | No | Partition | Unpadded month |

## Source ETL / code

`energy-etl` repo, `natgas_sap_etl/`. Lambda runs daily at 13:00 UTC (the SAP for a gas day is typically published at 12:00 UTC).

## Freshness & update cadence

Updated daily at 13:00 UTC (the SAP for a gas day is typically published at 12:00 UTC). The SAP is first published as an estimate (`E`), then refined to an actual (`A`) over subsequent days. The table is append-only — both estimate and actual revisions accumulate.

**History from:** ~5 years of rolling history is available from the API (approximately 2021-03-18 onwards). The API does not expose data older than its rolling window.

## Source & migration note

> The legacy SOAP API at `marketinformation.natgrid.co.uk` was decommissioned — the domain no longer resolves. This pipeline uses the replacement REST API. Monitor [apideveloper.nationalgas.com](https://apideveloper.nationalgas.com) and [data.nationalgas.com](https://data.nationalgas.com) for any further changes.

## Known issues & caveats

- **The table is append-only versioned.** A gas day typically has both an estimate row and a later actual row. Always filter to `applicable_at = MAX(applicable_at)` per `applicable_for` (gas day) to get the final published price for a day. Using all rows will double-count.
- **Partitioned by year/month only** (no day partition), unlike most other tables in this project. Athena partition pruning works at month granularity — queries filtering by day still scan the full month.
- The gas day runs 06:00–06:00 UTC (not midnight-to-midnight). `applicable_for` is the calendar date of the gas day start.
- `quality_indicator = 'L'` (Late) indicates the SAP was published after the normal deadline, which can occur around public holidays.
- Partition columns are unpadded strings.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
