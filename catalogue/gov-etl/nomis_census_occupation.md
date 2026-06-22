# Census 2021 Occupation (TS063)

## Description

Census 2021 counts of usual residents aged 16+ in employment, broken down by occupation
(SOC 2020 major group), for all 7,638 electoral wards in England and Wales. One row per
ward with 9 occupation group columns plus a total. Resident-based — counts where people
live, not where they work. Useful for understanding the occupational profile and skill mix
of neighbourhoods, and as a proxy for income levels.

## Classification

Public. Aggregated Census data; no personal data (minimum ward-level aggregation).

## Owner

Dan. See gov-etl repo for the code.

## Source & references

Nomis (ONS) — dataset NM_2080_1 (TS063 - Occupation), Census 2021.
https://www.nomisweb.co.uk/

## License & usage restrictions

Open Government Licence v3.0. Free to use with attribution to ONS/Nomis.

## Location

- S3: `s3://dantelore.data.incoming/nomis_census_occupation/occupation/occupation_2021.parquet`
- Glue: `incoming.nomis_census_occupation`
- Format: Single Parquet file, no partitioning (7,638 rows — always scanned in full)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| ward_cd | string | No | Primary; joins to nomis_census_industry on ward_cd | ONS ward code (e.g. `E05012132`) |
| ward_nm | string | No | | Ward name |
| total | int | Yes | | Total residents aged 16+ in employment |
| managers_directors_senior_officials | int | Yes | | SOC 2020 major group 1 |
| professional | int | Yes | | SOC 2020 major group 2 |
| associate_professional_technical | int | Yes | | SOC 2020 major group 3 |
| administrative_secretarial | int | Yes | | SOC 2020 major group 4 |
| skilled_trades | int | Yes | | SOC 2020 major group 5 |
| caring_leisure_service | int | Yes | | SOC 2020 major group 6 |
| sales_customer_service | int | Yes | | SOC 2020 major group 7 |
| process_plant_machine_operatives | int | Yes | | SOC 2020 major group 8 |
| elementary | int | Yes | | SOC 2020 major group 9 |

## Source ETL / code

`github.com/dantelore/gov-etl`, `nomis_census_occupation/nomis_census_occupation_load.py`.

## Freshness & update cadence

Census 2021 snapshot — static, will not change. The next Census is expected ~2031.

## Known issues & caveats

- Resident-based, not workplace-based. See `nomis_census_industry` caveats — same applies here.
- Ward boundaries are 2021 Census wards (TYPE153 in Nomis).
- Some counts may be suppressed or rounded by ONS for disclosure control.
- There is no direct join key to geography tables beyond `ward_cd` — to join to MSOA-level
  data you need a ward-to-MSOA lookup (not currently in this data lake).

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
