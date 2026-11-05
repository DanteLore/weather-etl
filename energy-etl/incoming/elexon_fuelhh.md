# incoming.elexon_fuelhh

## Description

Half-hourly electricity generation by fuel type in megawatts (MW), from Elexon's FUELHH (Fuel Type Half-Hourly) report. Covers all metered generation on the GB transmission network: thermal plant (CCGT, OCGT, coal, nuclear, oil, biomass), renewables (wind, hydro), pumped storage, and each named interconnector individually. This is the most granular source of generation mix data in this project — prefer it over `incoming.carbon_intensity_generation` for MW-level analysis.

## Classification

Public. Elexon BMRS data is published under the Balancing and Settlement Code and is freely accessible.

## Owner

Dan (energy-etl). Query via `incoming.elexon_fuelhh` in Athena.

## Source & references

- Elexon BMRS API: https://developer.data.elexon.co.uk/
- Report: FUELHH (Indicated Generation by Fuel Type, half-hourly).
- API key required (stored in `api_keys.py`, not in this repo).

## License & usage restrictions

Elexon open data licence. Free to use; attribution to Elexon preferred. No restrictions on internal use.

## Location

- S3: `s3://dantelore.data.incoming/elexon_fuelhh/`
- Glue / Athena: `incoming.elexon_fuelhh`
- Format: NDJSON (JsonSerDe), partitioned by year/month/day

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| settlement_date | string | No | Primary (with settlement_period) | Settlement date (YYYY-MM-DD) |
| settlement_period | int | No | Primary (with settlement_date) | Half-hour period 1–48 |
| start_time | string | No | | ISO 8601 UTC start of the period |
| publish_time | string | No | | UTC timestamp when Elexon published this data |
| gen_biomass | int | Yes | | Biomass generation (MW) |
| gen_ccgt | int | Yes | | Combined Cycle Gas Turbine generation (MW) |
| gen_coal | int | Yes | | Coal generation (MW) |
| gen_nuclear | int | Yes | | Nuclear generation (MW) |
| gen_ocgt | int | Yes | | Open Cycle Gas Turbine generation (MW) |
| gen_oil | int | Yes | | Oil generation (MW) |
| gen_other | int | Yes | | Other generation (MW) |
| gen_npshyd | int | Yes | | Non-pumped-storage hydro generation (MW) |
| gen_ps | int | Yes | | Pumped storage (MW; negative = charging from grid) |
| gen_wind | int | Yes | | Wind generation (MW) |
| gen_intelec | int | Yes | | ElecLink interconnector to France (MW; negative = export) |
| gen_intew | int | Yes | | East-West interconnector to Ireland (MW; negative = export) |
| gen_intfr | int | Yes | | IFA interconnector to France (MW; negative = export) |
| gen_intgrnl | int | Yes | | Greenlink interconnector to Ireland (MW; negative = export) |
| gen_intifa2 | int | Yes | | IFA2 interconnector to France (MW; negative = export) |
| gen_intirl | int | Yes | | Moyle interconnector to Northern Ireland (MW; negative = export) |
| gen_intned | int | Yes | | BritNed interconnector to Netherlands (MW; negative = export) |
| gen_intnem | int | Yes | | NEMO interconnector to Belgium (MW; negative = export) |
| gen_intnsl | int | Yes | | North Sea Link interconnector to Norway (MW; negative = export) |
| gen_intvkl | int | Yes | | Viking interconnector to Denmark (MW; negative = export) |
| year | string | No | Partition | Unpadded year |
| month | string | No | Partition | Unpadded month |
| day | string | No | Partition | Unpadded day |

## Source ETL / code

`energy-etl` repo, `elexon_fuelhh_etl/`. Lambda runs every 30 minutes, fetching the latest 48 hours. This means a missed run does not create a gap — the next run overwrites/fills the same periods.

## Freshness & update cadence

Updated every 30 minutes, covering the last 48 hours of data. The Lambda overwrites per-day files on each run, picking up late-published periods. Elexon publishes FUELHH data with a short lag; `publish_time` records when data became available.

**History from:** 2020-01-01. Typical volume: 48 rows/day (one per settlement period, wide format — pivoted from 20 fuel types × 48 periods).

## Running costs (estimated)

Estimated monthly cost in USD at AWS eu-west-1 list prices, from measurements taken 2026-11-05: the S3 size of this table's location, plus the last 30 days of Lambda metrics and Athena query history. Excludes tax and the AWS free tier (which currently covers all Lambda usage). Method: [energy-etl README](../README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.060 GB, 2,501 objects | $0.001 |
| Lambda `load_elexon_fuelhh_data` | 1,440 runs × 8.4 s × 128 MB | $0.025 |
| S3 writes | ~1,440 files + 4,309 partition-DDL result files | $0.050 |
| Athena (loader) | 4,309 `ADD PARTITION` statements — DDL is free | $0 |
| CloudWatch Logs | Lambda log ingestion | $0.002 |
| **Total** | | **$0.079** |

Querying it: a full scan reads about 0.06 GB, roughly <$0.001 per query (Athena bills a 10 MB minimum).

## Known issues & caveats

- Does **not** include embedded (behind-the-meter) generation. Wind and solar figures here are lower than the Carbon Intensity API's percentages, which include distributed generation. The two sources are not directly comparable.
- Pumped storage (`gen_ps`) is negative when absorbing power (charging). Do not sum it with generation columns without checking sign.
- Interconnector values are negative on export. "Net import" for a given interconnector is the raw field value — positive is import, negative is export.
- New interconnectors (e.g. Viking) have null values for periods before they came online. Do not interpret null as zero generation.
- Partition columns are unpadded strings.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active. Modelled into [`lake.elexon_fuelhh`](../lake/elexon_fuelhh.md) — use that for analysis.
