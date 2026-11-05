# lake.orr_network_rail_performance — ORR Performance by Network Rail Region and Route

## Description

Cancellations %, on time % and delay minutes per 1,000 train miles for each Network Rail region (5) and route (14), per rail period. Delay is split into Network Rail-attributable and operator-attributable. The Thatcham corridor is on the **Western** route, part of the **Wales and Western** region.

## Classification

Public. Official statistics with no personal data.

## Owner

Dan (data platform).

## Source & references

ORR table 3121, 3122, 3131, 3132, 3181a–c and 3182b–c: *Cancellations, on time and delay minutes per 1,000 train miles, by Network Rail region and route (periodic)*. Derived from [`incoming.orr_rail_performance`](../incoming/orr_rail_performance.md), which links to every source page. Original source: [ORR Data Portal](https://dataportal.orr.gov.uk/statistics/performance/passenger-rail-performance/).

## License & usage restrictions

Open Government Licence v3.0. Attribution requested: "Office of Rail and Road".

## Location

- S3: `s3://dantelore.data.lake/orr_network_rail_performance/`
- Glue: `lake.orr_network_rail_performance`
- Format: Parquet (ParquetHiveSerDe)
- Not partitioned (small table, 1,862 rows on 2026-11-05)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| financial_year_start | int | No | Key | Start year of the rail financial year (April to March), e.g. `2026` for 2026-27 |
| rail_period | int | No | Key | Rail period 1–13, roughly four weeks each |
| area_type | string | No | Key | `region` or `route` |
| area | string | No | Key | Region (`Eastern`, `North West and Central`, `Scotland`, `Southern`, `Wales and Western`) or route (e.g. `Western`, `Wessex`, `Anglia`) |
| cancellations_pct | double | Yes |  | Cancellations % |
| cancellations_pct_maa | double | Yes |  | Cancellations %, moving annual average |
| on_time_pct | double | Yes |  | % of station stops on time |
| on_time_pct_maa | double | Yes |  | % of station stops on time, moving annual average |
| delay_per_1000_train_miles | double | Yes |  | Delay minutes per 1,000 train miles, all causes |
| delay_per_1000_train_miles_maa | double | Yes |  | Delay minutes per 1,000 train miles, all causes, moving annual average |
| nr_delay_per_1000_train_miles | double | Yes |  | Network Rail-attributable delay minutes per 1,000 train miles |
| nr_delay_per_1000_train_miles_maa | double | Yes |  | Network Rail-attributable delay minutes per 1,000 train miles, moving annual average |
| operator_delay_per_1000_train_miles | double | Yes |  | Operator-attributable delay minutes per 1,000 train miles |
| operator_delay_per_1000_train_miles_maa | double | Yes |  | Operator-attributable delay minutes per 1,000 train miles, moving annual average |
| snapshot_date | string | No |  | Date the ORR release this row comes from was loaded |

Key: (`financial_year_start`, `rail_period`, `area_type`, `area`). Percentages are 0–100.

## Common queries

```sql
select financial_year_start, rail_period, area, on_time_pct, cancellations_pct,
       nr_delay_per_1000_train_miles, operator_delay_per_1000_train_miles
from lake.orr_network_rail_performance
where area_type = 'route'
  and financial_year_start * 100 + rail_period = (select max(financial_year_start * 100 + rail_period) from lake.orr_network_rail_performance)
order by on_time_pct
```

## Source ETL / code

`rail_data_model/sql/orr_network_rail_performance.sql` in the [rail-etl repo](https://github.com/DanteLore/rail-etl), run by the `model_rail_data` Lambda. It reads only the latest snapshot of each source table, which holds the full history including ORR's revisions, and pivots ORR's one-row-per-cell incoming layout into columns. Cells with no number (`[z]` or empty) are null.

## Freshness & update cadence

Full rebuild daily at 05:45 UTC by `model_rail_data`, after the 05:15 ORR load. The data itself changes about every four weeks, when ORR publishes a rail period (3–4 weeks after it ends).

**History from:** 2019-20 P01.

## Running costs (estimated)

Estimated monthly cost in USD at AWS eu-west-1 list prices, from the rebuild measured on 2026-11-05. Excludes tax and the AWS free tier (which currently covers all Lambda usage). Method: [rail-etl README](../README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | Under 1 MB, 1 object | <$0.001 |
| Lambda `model_rail_data` (share) | 30 rebuilds × ~3 s × 128 MB | <$0.001 |
| Athena (rebuild) | 30 × 10 MB (the per-query minimum; it scans under 1 MB) | $0.002 |
| S3 reads (rebuild) | 30 × the source table's snapshot files | <$0.001 |
| **Total** |  | **$0.002** |

Querying it: a full scan reads under 1 MB, roughly $0.00005 per query (Athena bills a 10 MB minimum).

## Known issues & caveats

- **Route totals for delay per 1,000 train miles are derived.** ORR's table 3182a (route totals) repeats 3182c (operator-attributable) in every cell checked on 2026-11-05. By region, 3181a equals 3181b + 3181c to within rounding in all 920 cells, so the route total here is `nr_ + operator_delay_per_1000_train_miles`, rounded to 0.1. `incoming.orr_rail_performance` keeps 3182a as published.
- The `High Speed` route has no cancellations or delay figures, and its `on_time_pct` is published as 0.00 in every period, so treat it as no data.
- Area names are title-cased, because ORR's column headers use different cases in different tables.
- Rail periods have no calendar dates in the ORR tables: P1 starts on 1 April and P13 ends on 31 March, so their lengths vary.

## Interested parties

None yet - new dataset.

## Status

Active.
