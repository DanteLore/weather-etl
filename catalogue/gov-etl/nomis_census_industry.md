# Census 2021 Industry (TS060)

## Description

Census 2021 counts of usual residents aged 16+ in employment, broken down by industry
sector (SIC section), for all 7,638 electoral wards in England and Wales. One row per ward
with 18 industry group columns plus a total. Resident-based — counts where people live, not
where they work. Useful for understanding the economic character of neighbourhoods.

## Classification

Public. Aggregated Census data; no personal data (minimum ward-level aggregation).

## Owner

Dan. See gov-etl repo for the code.

## Source & references

Nomis (ONS) — dataset NM_2077_1 (TS060 - Industry), Census 2021.
https://www.nomisweb.co.uk/

## License & usage restrictions

Open Government Licence v3.0. Free to use with attribution to ONS/Nomis.

## Location

- S3: `s3://dantelore.data.incoming/nomis_census_industry/industry/industry_2021.parquet`
- Glue: `incoming.nomis_census_industry`
- Format: Single Parquet file, no partitioning (7,638 rows — always scanned in full)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| ward_cd | string | No | Primary | ONS ward code (e.g. `E05012132`) |
| ward_nm | string | No | | Ward name |
| total | int | Yes | | Total residents aged 16+ in employment |
| agriculture_forestry_fishing | int | Yes | | SIC section A |
| mining_quarrying | int | Yes | | SIC section B |
| manufacturing | int | Yes | | SIC section C |
| electricity_gas_steam | int | Yes | | SIC section D |
| water_sewerage_waste | int | Yes | | SIC section E |
| construction | int | Yes | | SIC section F |
| wholesale_retail | int | Yes | | SIC section G |
| transport_storage | int | Yes | | SIC section H |
| accommodation_food | int | Yes | | SIC section I |
| information_communication | int | Yes | | SIC section J |
| financial_insurance | int | Yes | | SIC section K |
| real_estate | int | Yes | | SIC section L |
| professional_scientific | int | Yes | | SIC section M |
| administrative_support | int | Yes | | SIC section N |
| public_administration_defence | int | Yes | | SIC section O |
| education | int | Yes | | SIC section P |
| health_social_work | int | Yes | | SIC section Q |
| other | int | Yes | | SIC sections R, S, T, U combined |

## Source ETL / code

`github.com/dantelore/gov-etl`, `nomis_census_industry/nomis_census_industry_load.py`.

## Freshness & update cadence

Census 2021 snapshot — static, will not change. The next Census is expected ~2031.

## Known issues & caveats

- Resident-based, not workplace-based. A ward with many commuters will show their industry
  in the ward where they live, not where they work.
- Ward boundaries used are the 2021 Census wards (TYPE153 in Nomis). These differ from
  pre-2023 ward boundaries used in some other datasets.
- Some counts may be suppressed or rounded by ONS for disclosure control.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
