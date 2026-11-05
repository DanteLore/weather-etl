# incoming.natgas_sap

## Description

National Gas System Average Price (SAP) — the volume-weighted average price of natural gas traded on the GB gas network for each gas day, in pence per kilowatt hour (p/kWh). Published daily by National Gas Transmission via their REST API (publication PUBOB603). The SAP is a widely-used reference price for gas imbalance settlement and short-term gas market analysis. There is one record per gas day: each run overwrites the current and previous month's files, so a revised price replaces the earlier value rather than adding a row. For analysis use [`lake.natgas_sap`](../lake/natgas_sap.md).

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
| applicable_for | string | No | Primary | Gas day date the price applies to (YYYY-MM-DD) |
| applicable_at | string | No | | UTC datetime (`YYYY-MM-DD HH:MM`) when this value became applicable — the day after the gas day |
| value | double | No | | System Average Price in pence per kilowatt hour (p/kWh) |
| quality_indicator | string | No | | Quality code from National Gas. Observed values: `L` (2,054 of 2,060 days) and `A` (6 days) — see caveats |
| substituted | string | No | | `Y` if the value has been substituted, `N` otherwise |
| create_date | string | No | | UTC datetime when this record was created/published by National Gas |
| year | string | No | Partition | Unpadded year |
| month | string | No | Partition | Unpadded month |

## Source ETL / code

`energy-etl` repo, `natgas_sap_etl/`. Lambda runs daily at 13:00 UTC (the SAP for a gas day is typically published at 12:00 UTC).

## Freshness & update cadence

Updated daily at 13:00 UTC (the SAP for a gas day is typically published at 12:00 UTC). Each run re-fetches the current and previous calendar month and overwrites both monthly files, so revisions within that window are picked up automatically. Older months are not re-fetched.

**History from:** ~5 years of rolling history is available from the API (approximately 2021-03-18 onwards). The API does not expose data older than its rolling window.

## Source & migration note

> The legacy SOAP API at `marketinformation.natgrid.co.uk` was decommissioned — the domain no longer resolves. This pipeline uses the replacement REST API. Monitor [apideveloper.nationalgas.com](https://apideveloper.nationalgas.com) and [data.nationalgas.com](https://data.nationalgas.com) for any further changes.

## Known issues & caveats

- **One row per gas day, not versioned.** Monthly files are overwritten on each run, so there is no estimate/actual history — only the latest value fetched. (Checked 2026-11-04: 2,060 rows, 2,060 distinct gas days, continuous from 2021-03-15.)
- **Partitioned by year/month only** (no day partition), unlike most other tables in this project. Athena partition pruning works at month granularity — queries filtering by day still scan the full month.
- The gas day runs 06:00–06:00 UTC (not midnight-to-midnight). `applicable_for` is the calendar date of the gas day start.
- `quality_indicator` is `L` on almost every day (2,054 of 2,060) and `A` on 6; no `E` values have been seen. The meaning of `L` here is unconfirmed — it is too common to mean "late publication". Check National Gas's PUBOB603 documentation before relying on it.
- Partition columns are unpadded strings.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active. Modelled into [`lake.natgas_sap`](../lake/natgas_sap.md) — use that for analysis.
