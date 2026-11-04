# repd

## Description

Renewable Energy Planning Database (REPD) — every known UK renewable and low-carbon electricity generating project and its planning/development status, as published quarterly by the Department for Energy Security and Net Zero (DESNZ). Covers onshore and offshore wind, solar, biomass, hydro, tidal, and other technologies from initial planning application through to operational. Use this to understand the pipeline of new capacity, track project status over time, or find plant details (location, capacity, developer).

## Classification

Public. Published as open data by DESNZ under the Open Government Licence.

## Owner

Dan (energy-etl). Query via `incoming.repd` in Athena.

## Source & references

- DESNZ REPD download page: https://www.gov.uk/government/publications/renewable-energy-planning-database-monthly-extract
- Published quarterly (approximately). Downloaded as CSV and ingested as NDJSON.
- No authentication required.

## License & usage restrictions

Open Government Licence v3.0. Free to use, share and adapt with attribution to DESNZ. No restrictions on internal use.

## Location

- S3: `s3://dantelore.data.incoming/repd/`
- Glue / Athena: `incoming.repd`
- Format: NDJSON (JsonSerDe), partitioned by year/month/day (of ingest)

## Field spec

All fields are stored as strings — the source CSV is ingested verbatim. Cast numeric fields (`installed_capacity_mwelec`, coordinates, etc.) at query time.

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| ref_id | string | Yes | Primary (with repd_release_date) | REPD unique project identifier — stable across releases |
| old_ref_id | string | Yes | | Legacy identifier from the previous numbering scheme |
| repd_release_date | string | No | Primary (with ref_id) | Quarter release date (e.g. `2024-10-01`); use with ref_id as composite key |
| record_last_updated_dd_mm_yyyy | string | Yes | | Date the record was last updated by DESNZ (DD/MM/YYYY) |
| operator_or_applicant | string | Yes | | Developer / operator name |
| site_name | string | Yes | | Project name |
| technology_type | string | Yes | | Technology (e.g. `Wind Onshore`, `Solar Photovoltaics`, `Biomass (dedicated)`) |
| storage_type | string | Yes | | Battery storage type if co-located |
| storage_co_location_repd_ref_id | string | Yes | | ref_id of associated storage project |
| installed_capacity_mwelec | string | Yes | | Nameplate electrical capacity (MW) — cast to double at query time; may be null in early planning |
| development_status | string | Yes | | Full development status description |
| development_status_short | string | Yes | | Short status code |
| technology_type | string | Yes | | Technology category |
| address | string | Yes | | Site address |
| county | string | Yes | | County |
| region | string | Yes | | Region |
| country | string | Yes | | Country within UK |
| post_code | string | Yes | | Postcode |
| x_coordinate | string | Yes | | OS National Grid Easting (OSGB36 / EPSG:27700) — convert with pyproj; ~5–10% of rows missing |
| y_coordinate | string | Yes | | OS National Grid Northing (OSGB36 / EPSG:27700) |
| planning_authority | string | Yes | | Planning authority responsible |
| planning_application_reference | string | Yes | | Planning application reference number |
| cfd_allocation_round | string | Yes | | Contracts for Difference allocation round, if applicable |
| ro_banding_roc_mwh | string | Yes | | Renewables Obligation banding (ROC/MWh) |
| turbine_capacity_mw | string | Yes | | Per-turbine rated capacity (MW) — null for non-wind |
| no_of_turbines | string | Yes | | Number of turbines — null for non-wind |
| height_of_turbines_m | string | Yes | | Turbine tip height in metres — null for non-wind |
| mounting_type_for_solar | string | Yes | | Ground mount, rooftop, etc. — null for non-solar |
| planning_application_submitted | string | Yes | | Date planning application was submitted (DD/MM/YYYY) |
| planning_permission_granted | string | Yes | | Date planning permission was granted |
| under_construction | string | Yes | | Date construction started |
| operational | string | Yes | | Date the project became operational |
| ingested_at | string | No | | UTC timestamp when the pipeline ran |
| year | string | No | Partition | Unpadded year of ingest |
| month | string | No | Partition | Unpadded month of ingest |
| day | string | No | Partition | Unpadded day of ingest |

(Several additional planning timeline fields exist — appeal dates, Secretary of State intervention dates, etc. — all stored as strings.)

## Source ETL / code

`energy-etl` repo, `repd/repd_load.py`. Run manually: update `CSV_URL` in the script and run `python repd/repd_load.py`. Column names are normalised from the original CSV headers via `normalise_col()`.

Raw source CSV files are archived at `s3://dantelore.data.incoming/repd_raw/` before transformation.

## Freshness & update cadence

Updated quarterly (approximately January, April, July, October). There is no automatic check for new releases — the pipeline must be triggered manually when a new REPD is published. Use `repd_release_date` to identify which quarter's data you are looking at.

**History from:** First load: Q4 2025 (January 2026 release). Earlier quarters can be loaded from the DESNZ archive if needed.

## Known issues & caveats

- **All columns are stored as strings.** Cast `installed_capacity_mwelec` and coordinate fields to numeric types at query time. Nulls, blank strings, and `-` values appear in numeric columns for early-stage projects.
- **~5–10% of rows have missing coordinates** (`x_coordinate`/`y_coordinate`). Do not interpret null as zero — the project exists, the coordinates are just not recorded yet.
- **Coordinates are OSGB36 (Easting/Northing), not WGS84.** Convert to lat/lon using pyproj or equivalent before plotting or joining to lat/lon-based datasets.
- **The composite primary key is `(ref_id, repd_release_date)`.** Each quarterly release is a full snapshot. A project may appear in every release from application through to decommissioning with different status values.
- **`record_last_updated_dd_mm_yyyy` is DD/MM/YYYY** — do not parse as ISO 8601.
- Partition columns are unpadded strings.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
