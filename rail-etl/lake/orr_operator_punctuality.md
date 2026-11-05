# lake.orr_operator_punctuality — ORR Punctuality by Operator

## Description

Punctuality at recorded station stops per rail period for every operator and Great Britain: the share of stops reached early, on time (within 59 seconds), within 3, 5, 10, 15, 20 and 30 minutes, and the share 15, 20 or 30 or more minutes late. This is the industry's main punctuality measure since 2019, and the benchmark for `lake.rtt_service_calls.is_on_time`.

## Classification

Public. Official statistics with no personal data.

## Owner

Dan (data platform).

## Source & references

ORR table 3138: *Train punctuality at recorded station stops by operator (periodic)*. Derived from [`incoming.orr_rail_performance`](../incoming/orr_rail_performance.md), which links to every source page. Original source: [ORR Data Portal](https://dataportal.orr.gov.uk/statistics/performance/passenger-rail-performance/).

## License & usage restrictions

Open Government Licence v3.0. Attribution requested: "Office of Rail and Road".

## Location

- S3: `s3://dantelore.data.lake/orr_operator_punctuality/`
- Glue: `lake.orr_operator_punctuality`
- Format: Parquet (ParquetHiveSerDe)
- Not partitioned (small table, 4,297 rows on 2026-11-05)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| financial_year_start | int | No | Key | Start year of the rail financial year (April to March), e.g. `2026` for 2026-27 |
| rail_period | int | No | Key | Rail period 1–13, roughly four weeks each |
| geography_type | string | No |  | `national`, `sector` or `operator`. National and sector rows are ORR's aggregate rows, e.g. `Great Britain`, `Franchised Long Distance`, `London and South East` |
| operator | string | No | Key | Operator, or the aggregate's name for national and sector rows, as ORR spells it in this table |
| station_stops | bigint | Yes |  | Recorded station stops in the period |
| early_pct | double | Yes |  | % of stops reached early |
| on_time_pct | double | Yes |  | % of stops reached early or within 59 seconds of the timetable |
| time_to_3_pct | double | Yes |  | % of stops reached early or less than 3 minutes late (ORR "Time to 3") |
| time_to_5_pct | double | Yes |  | % of stops reached early or less than 5 minutes late (ORR "Time to 5") |
| time_to_10_pct | double | Yes |  | % of stops reached early or less than 10 minutes late (ORR "Time to 10") |
| time_to_15_pct | double | Yes |  | % of stops reached early or less than 15 minutes late (ORR "Time to 15") |
| time_to_20_pct | double | Yes |  | % of stops reached early or less than 20 minutes late (ORR "Time to 20") |
| time_to_30_pct | double | Yes |  | % of stops reached early or less than 30 minutes late (ORR "Time to 30") |
| late_15_plus_pct | double | Yes |  | % of stops reached 15 or more minutes late |
| late_20_plus_pct | double | Yes |  | % of stops reached 20 or more minutes late |
| late_30_plus_pct | double | Yes |  | % of stops reached 30 or more minutes late |
| early_pct_maa … late_30_plus_pct_maa | double | Yes |  | Moving annual average of each of the 11 percentages above (11 columns) |
| snapshot_date | string | No |  | Date the ORR release this row comes from was loaded |

Key: (`financial_year_start`, `rail_period`, `operator`). Percentages are 0–100.

## Common queries

```sql
select financial_year_start, rail_period, operator, on_time_pct, time_to_3_pct, time_to_15_pct
from lake.orr_operator_punctuality
where operator in ('Great Western Railway', 'Great Britain')
order by financial_year_start desc, rail_period desc, operator
limit 26
```

## Source ETL / code

`rail_data_model/sql/orr_operator_punctuality.sql` in the [rail-etl repo](https://github.com/DanteLore/rail-etl), run by the `model_rail_data` Lambda. It reads only the latest snapshot of each source table, which holds the full history including ORR's revisions, and pivots ORR's one-row-per-cell incoming layout into columns. Cells with no number (`[z]` or empty) are null.

## Freshness & update cadence

Full rebuild daily at 05:45 UTC by `model_rail_data`, after the 05:15 ORR load. The data itself changes about every four weeks, when ORR publishes a rail period (3–4 weeks after it ends).

**History from:** 2014-15 P01.

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

- Measured at arrival at every recorded station stop, not just the destination.
- The "Time to N" thresholds are as ORR defines them: early, or less than N minutes late.
- Rail periods have no calendar dates in the ORR tables: P1 starts on 1 April and P13 ends on 31 March, so their lengths vary.

## Interested parties

None yet - new dataset.

## Status

Active.
