# elexon_pn

## Description

Physical Notifications (PN) from Elexon's Balancing Mechanism — the planned MW output profiles submitted by BM units ahead of each settlement period. Each row is one output level segment (a time/level pair) within a unit's notification for a given period. Physical Notifications represent what a generator intended to produce before any balancing instructions were issued. Use this alongside `incoming.elexon_boalf` to compare planned vs. instructed output, or to understand a unit's gate closure position.

## Classification

Public. Elexon BMRS data is published under the Balancing and Settlement Code and is freely accessible.

## Owner

Dan (energy-etl). Query via `incoming.elexon_pn` in Athena.

## Source & references

- Elexon BMRS API: https://developer.data.elexon.co.uk/
- Report: PN (Physical Notifications).
- API key required (stored in `api_keys.py`, not in this repo).

## License & usage restrictions

Elexon open data licence. Free to use; attribution to Elexon preferred. No restrictions on internal use.

## Location

- S3: `s3://dantelore.data.incoming/elexon_pn/`
- Glue / Athena: `incoming.elexon_pn`
- Format: Parquet (Snappy), partitioned by year/month/day

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| settlement_date | string | No | Primary (composite) | Settlement date (YYYY-MM-DD) |
| settlement_period | int | No | Primary (composite) | Half-hour period 1–48 |
| bm_unit | string | No | Primary (composite) | BM unit ID (e.g. `T_DRAXX-1`) |
| national_grid_bm_unit | string | Yes | | National Grid's equivalent BM unit identifier |
| time_from | string | No | | ISO 8601 UTC start of the output level segment |
| time_to | string | No | | ISO 8601 UTC end of the output level segment |
| level_from | double | No | | MW output level at the start of the segment |
| level_to | double | No | | MW output level at the end of the segment |
| ingested_at | string | No | | UTC timestamp when this record was written to S3 |
| year | string | No | Partition | Unpadded year |
| month | string | No | Partition | Unpadded month |
| day | string | No | Partition | Unpadded day |

## Source ETL / code

`energy-etl` repo, `elexon_pn_etl/`. Lambda runs daily at 06:00 UTC, fetching the previous day's data.

## Freshness & update cadence

Updated daily at 06:00 UTC with the previous day's data. Approximately one day of lag.

**History from:** 2020-01-01. Typical volume: ~125,000 rows/day. Storage format is Parquet (Snappy) — ~800 KB/day.

## Known issues & caveats

- A single BM unit may submit multiple PN segments per period (a piecewise linear profile). Sum or interpolate `level_from`/`level_to` across segments to reconstruct the full output profile within a period.
- PNs are submitted before gate closure and may differ substantially from actual metered output after balancing instructions.
- **The API does not expose PN revision history.** Each fetch returns the current snapshot at fetch time. The Lambda overwrites the file for each settlement date on every run. `ingested_at` records when the snapshot was taken, not when the PN was originally submitted to Elexon.
- Partition columns are unpadded strings.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
