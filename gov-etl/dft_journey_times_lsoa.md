# DfT Journey Time Statistics by LSOA (JTS0501–JTS0509)

## Description

The Department for Transport's modelled journey times from every neighbourhood (LSOA) in England to key services: employment centres, primary and secondary schools, further education, GPs, hospitals, food stores, town centres and pharmacies. For each service and mode (public transport or walk, cycle, car, and walking alone) it gives the minimum travel time to the nearest one, how many can be reached within 15, 30, 45 and 60 minutes, and the share of people within those times. Also known as the accessibility statistics or JTS tables. Long format: one row per LSOA, table and field.

## Classification

Public. Official statistics (outside the scope of National Statistics); no personal data.

## Owner

Dan. See the gov-etl repo for the code.

## Source & references

gov.uk "Journey time statistics: data tables (JTS)", the LSOA tables JTS0501 to JTS0509: https://www.gov.uk/government/statistical-data-sets/journey-time-statistics-data-tables-jts. Method: https://www.gov.uk/government/publications/journey-time-statistics-guidance

## License & usage restrictions

Open Government Licence v3.0. Attribute DfT.

## Location

- S3: `s3://dantelore.data.incoming/dft_journey_times_lsoa/edition_year=<yyyy>/dft_journey_times_lsoa_<table_id>.parquet`
- Glue: `incoming.dft_journey_times_lsoa`
- Format: Parquet, partitioned by `edition_year`, the year the figures describe

Each table's latest year was loaded:

| table_id | Service | edition_year | Fields |
|---|---|---|---|
| jts0501 | Employment centres (100–499, 500–4,999 and 5,000+ jobs) | **2017** | 82 |
| jts0502 | Primary schools | 2019 | 37 |
| jts0503 | Secondary schools | 2019 | 37 |
| jts0504 | Further education | 2019 | 37 |
| jts0505 | GPs | 2019 | 37 |
| jts0506 | Hospitals | 2019 | 37 |
| jts0507 | Food stores | 2019 | 37 |
| jts0508 | Town centres | 2019 | 37 |
| jts0509 | Pharmacies (cycle and car only) | **2015** | 19 |

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| lsoa_code | string | No | Key | LSOA (2011 boundaries), e.g. `E01016254` |
| region | string | No | | Region |
| la_code | string | No | | Local authority code at the time, e.g. `E06000037` |
| la_name | string | No | | Local authority name |
| table_id | string | No | Key | `jts0501` … `jts0509` |
| table_title | string | No | | The table's title on gov.uk |
| field | string | No | Key | DfT's field name, e.g. `GPPTt`, `TownCar30n`, `Empl_pop` |
| field_description | string | No | | DfT's description, e.g. `Travel time in minutes to nearest GP by PT/walk`, `% users within 30 minutes of town centres by car` |
| mode | string | Yes | | `public transport or walk`, `cycle`, `car` or `walk`; null for the service-users field |
| value | double | Yes | | Minutes, a count of destinations, a percentage, or a number of people, depending on the field |
| edition_year | string | No | Partition key | Year the figures describe |

Field names follow a pattern: service prefix (`GP`, `Town`, `Food`, `Hosp`, `PS`, `SS`, `FE`, `Empl`/`500Emp` etc., `Ph`), mode (`PT`, `Cyc`, `Car`, `Walk`), then `t` (minutes to the nearest), `15n`/`30n`/… (destinations within that time) or `15pct`/… (% of users within that time). `<prefix>_pop` is the number of service users in the LSOA (e.g. households, or children of school age).

## Common queries

Average minutes to the nearest GP and town centre for Thatcham-area LSOAs, by mode (use `la_name = 'West Berkshire'` for the district):

```sql
select table_id, field_description, mode, round(avg(value), 1) as avg_minutes
from incoming.dft_journey_times_lsoa
where edition_year = '2019' and la_name = 'West Berkshire'
  and table_id in ('jts0505', 'jts0508')
  and field_description like 'Travel time in minutes%'
group by 1, 2, 3 order by 1, 3
```

Across West Berkshire in 2019, the nearest town centre was 13 minutes by car on average, 29 by public transport or walking and 53 on foot.

## Source ETL / code

`github.com/dantelore/gov-etl`, `dft_journey_times/dft_journey_times_load.py`. It reads the attachment list from the gov.uk page, takes each table's newest year sheet, and attaches field descriptions from the table's metadata sheet.

## Freshness & update cadence

**The latest edition is 2019, published 4 November 2021.** DfT hasn't published JTS since; a user engagement exercise in March 2025 discussed its future, and DfT's newer "Transport connectivity metric" (September 2025) may replace it. Employment centres stop at 2017 and pharmacies at 2015. Rerun the loader if a new edition appears (it would land in a new `edition_year` partition).

## Running costs (estimated)

From the S3 size on 2026-11-12. Method: [gov-etl README](README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.058 GB, 9 objects | $0.001 |
| **Total** | | **$0.001** |

## Known issues & caveats

- **Old data.** Public transport times use 2019 timetables (2017 for jobs), before the pandemic and before later bus cuts. West Berkshire's bus use per head fell 27% between 2018/19 and 2024/25 ([dft_bus_statistics_la](dft_bus_statistics_la.md)), so current public transport times are probably worse.
- Modelled, not measured: the fastest door-to-door trip in a weekday morning window, from the LSOA's population-weighted centre.
- Times are capped at 120 minutes, so averages for walking to sparse services understate.
- LSOAs are 2011 boundaries and local authorities those of the edition year; join to 2021 LSOAs with care.
- England only.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|
| Thatcham analysis (thatchamplan.co.uk) | Dan | Access to services from the NE Thatcham area |

## Status

Active. Source no longer updated (see Freshness).
