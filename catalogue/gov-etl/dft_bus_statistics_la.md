# DfT Local Bus Statistics by Local Authority

## Description

The Department for Transport's annual local bus statistics for each local authority in England: passenger journeys (total, per head of population, and elderly and disabled concessionary journeys), vehicle miles run by commercial and council-supported services, and punctuality (the share of non-frequent services on time, and excess waiting time on frequent services). Financial years from 2004/05 or 2009/10 to 2024/25. Long format: one row per table, area, service type and year. For comparing bus provision and use in West Berkshire with similar areas.

## Classification

Public. Official statistics; no personal data.

## Owner

Dan. See the gov-etl repo for the code.

## Source & references

gov.uk "Bus statistics data tables": https://www.gov.uk/government/statistical-data-sets/bus-statistics-data-tables. Workbooks `bus01.ods` (passenger journeys), `bus02_mi.ods` (vehicle miles) and `bus09.ods` (reliability and punctuality). Notes on each table are in the workbooks' Notes sheets.

## License & usage restrictions

Open Government Licence v3.0. Attribute DfT.

## Location

- S3: `s3://dantelore.data.incoming/dft_bus_statistics_la/table_id=<table>/dft_bus_statistics_la_<table>.parquet`
- Glue: `incoming.dft_bus_statistics_la`
- Format: Parquet, partitioned by `table_id`

| table_id | Measure | Unit | Years (ending March) |
|---|---|---|---|
| bus01e | Passenger journeys on local bus services | millions | 2010–2025 |
| bus01f | Passenger journeys per head of population | journeys | 2010–2025 |
| bus01g | Elderly and disabled concessionary journeys | millions | 2010–2025 |
| bus01h | Concessionary journeys as a share of all journeys | % | 2010–2025 |
| bus02d_mi | Vehicle miles, by service type (commercial, local authority supported, total) | millions | 2010–2025 |
| bus09a | Non-frequent services running on time (former NI 178a) | % | 2005–2025 |
| bus09b | Average excess waiting time on frequent services (former NI 178b) | minutes | 2005–2025 |

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| table_title | string | No | | The table's title |
| unit | string | No | | E.g. `Passenger journeys (millions)`, `Percentage`, `Minutes` |
| area_code | string | No | Key | Local authority code (e.g. `E06000037`), or a region or England code for the subtotal rows |
| area_name | string | No | | E.g. `West Berkshire` |
| region | string | Yes | | Region (bus09b only) |
| service_type | string | Yes | Key | `Commercial`, `Local authority supported` or `Total` (bus02d_mi only) |
| year_ending_march | bigint | No | Key | Financial year, by its end: `2025` is April 2024 to March 2025 |
| year_label | string | No | | The column heading, including revision flags, e.g. `2019 [r] [note 6]` |
| value | double | Yes | | The figure; null where not available |
| value_marker | string | Yes | | DfT's marker where there's no figure; in this edition always `[z]` (not applicable or not reported) |
| notes | string | Yes | | The row's notes column, e.g. `[r] [note 7]` |
| edition_published | string | No | | When the gov.uk page was last updated, e.g. `2026-09-22` |
| table_id | string | No | Partition key | See the table above |

## Common queries

Bus journeys per head, West Berkshire against neighbouring and similar authorities, before the pandemic and now:

```sql
select area_name, year_ending_march, round(value, 1) as journeys_per_head
from incoming.dft_bus_statistics_la
where table_id = 'bus01f' and year_ending_march in (2019, 2025)
  and area_name in ('West Berkshire', 'Wokingham', 'Reading', 'Bracknell Forest', 'Wiltshire', 'Hampshire', 'Oxfordshire')
order by 1, 2
```

West Berkshire fell from 18.8 journeys per head in 2018/19 to 13.8 in 2024/25 (Reading: 128.7 to 109.5).

Punctuality of non-frequent services in West Berkshire:

```sql
select year_ending_march, value as pct_on_time, value_marker
from incoming.dft_bus_statistics_la
where table_id = 'bus09a' and area_name = 'West Berkshire'
order by 1
```

## Source ETL / code

`github.com/dantelore/gov-etl`, `dft_bus_statistics/dft_bus_statistics_load.py`. Finds the workbooks on the gov.uk page and parses each local authority sheet from its header row. It uses the `odf` engine because `bus09.ods` doesn't open with the faster calamine reader.

## Freshness & update cadence

Annual; DfT publishes a new edition each autumn and revises earlier years. Loaded 2026-11-12 from the edition published 22 September 2026 (data to 2024/25). Rerun the loader after each release: it overwrites each table.

## Running costs (estimated)

From the S3 size on 2026-11-12. Method: [gov-etl README](README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.0002 GB, 7 objects | <$0.001 |
| **Total** | | **<$0.001** |

## Known issues & caveats

- **Punctuality is self-reported** by local authorities, often from small samples, and many authorities stopped reporting; West Berkshire reported excess waiting time on frequent services (bus09b) only for 2014/15 and 2016/17. The BODS-based [lake.bus_service_calls](../rail-etl/lake/bus_service_calls.md) measures punctuality directly for the Thatcham area.
- Figures from 2021/22 onwards are provisional and get revised.
- 2020/21 and 2021/22 are distorted by the pandemic.
- Areas are as published: some rows are regions or combined authorities, and boundaries changed (e.g. Northamptonshire in 2021).
- Tables by metropolitan status, region and fares (BUS01a–d, BUS02a–b and the other workbooks) weren't loaded.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|
| Thatcham analysis (thatchamplan.co.uk) | Dan | West Berkshire against similar areas |

## Status

Active.
