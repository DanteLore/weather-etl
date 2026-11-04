# elexon_boalf

## Description

Bid-Offer Acceptance Level File (BOALF) from Elexon's Balancing Mechanism — the actual instructions issued by National Grid to BM units to increase or decrease output, and the MW levels those instructions set. Each row is one accepted instruction (acceptance) for one BM unit. This shows what the system operator actually did to balance the grid, as opposed to `incoming.elexon_bod` which shows what was available. Use this for balancing cost analysis or to understand which plant was instructed up or down in any given period.

## Classification

Public. Elexon BMRS data is published under the Balancing and Settlement Code and is freely accessible.

## Owner

Dan (energy-etl). Query via `incoming.elexon_boalf` in Athena.

## Source & references

- Elexon BMRS API: https://developer.data.elexon.co.uk/
- Report: BOALF (Bid-Offer Acceptance Level File).
- API key required (stored in `api_keys.py`, not in this repo).

## License & usage restrictions

Elexon open data licence. Free to use; attribution to Elexon preferred. No restrictions on internal use.

## Location

- S3: `s3://dantelore.data.incoming/elexon_boalf/`
- Glue / Athena: `incoming.elexon_boalf`
- Format: Parquet (Snappy), partitioned by year/month/day

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| settlement_date | string | No | Primary (composite) | Settlement date (YYYY-MM-DD) |
| settlement_period_from | int | No | | Settlement period at the start of the acceptance window (1–48) |
| settlement_period_to | int | No | | Settlement period at the end of the acceptance window |
| bm_unit | string | No | Primary (composite) | BM unit ID (e.g. `T_DRAXX-1`) |
| national_grid_bm_unit | string | Yes | | National Grid's equivalent BM unit identifier |
| acceptance_number | int | No | Primary (composite) | Elexon acceptance sequence number — unique per unit per day |
| acceptance_time | string | No | | UTC timestamp when the acceptance was issued |
| time_from | string | No | | ISO 8601 UTC start of the instruction's output window |
| time_to | string | No | | ISO 8601 UTC end of the instruction's output window |
| level_from | double | No | | MW level at the start of the window |
| level_to | double | No | | MW level at the end of the window |
| deemed_bo_flag | boolean | Yes | | True if this acceptance was deemed (not physically dispatched) |
| so_flag | boolean | Yes | | True if instructed for system (SO) reasons rather than energy balancing |
| stor_flag | boolean | Yes | | True if this is a STOR (Short Term Operating Reserve) instruction |
| rr_flag | boolean | Yes | | True if this is a Response and Reserve (RR) instruction |
| amendment_flag | string | Yes | | Amendment code if this acceptance was later modified |
| year | string | No | Partition | Unpadded year |
| month | string | No | Partition | Unpadded month |
| day | string | No | Partition | Unpadded day |

## Source ETL / code

`energy-etl` repo, `elexon_boalf_etl/`. Lambda runs daily at 06:00 UTC, fetching the previous day's data.

## Freshness & update cadence

Updated daily at 06:00 UTC with the previous day's data. Approximately one day of lag.

**History from:** 2020-01-01. Typical volume: ~18,000 rows/day. The Elexon API endpoint is BOALF (Final) — there is no plain BOAL endpoint.

## Known issues & caveats

- `so_flag = true` indicates the instruction was for system security rather than energy imbalance. Costs associated with SO-flagged acceptances are excluded from the cash-out price calculation. Separate these if doing energy-only balancing cost analysis.
- Acceptances can span multiple settlement periods (`settlement_period_from` ≠ `settlement_period_to`).
- Joining to `elexon_bod` on `(settlement_date, bm_unit)` gives the bid-offer prices that apply to each acceptance.
- Partition columns are unpadded strings.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
