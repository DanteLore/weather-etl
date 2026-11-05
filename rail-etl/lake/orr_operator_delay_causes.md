# lake.orr_operator_delay_causes — ORR Delay Minutes by Operator and Cause

## Description

Delay minutes per rail period for each operator, split by who was responsible and the cause. `nr_on_toc` is Network Rail delaying the operator (track, non-track assets, severe weather, external, network management). `toc_on_self` is the operator delaying itself (fleet, traincrew, operations, stations, other). `toc_on_toc` is another operator delaying it. Use it to see why GWR's trains are late.

## Classification

Public. Official statistics with no personal data.

## Owner

Dan (data platform).

## Source & references

ORR table 3184: *Delay minutes by operator and cause (periodic)*. Derived from [`incoming.orr_rail_performance`](../incoming/orr_rail_performance.md), which links to every source page. Original source: [ORR Data Portal](https://dataportal.orr.gov.uk/statistics/performance/passenger-rail-performance/).

## License & usage restrictions

Open Government Licence v3.0. Attribution requested: "Office of Rail and Road".

## Location

- S3: `s3://dantelore.data.lake/orr_operator_delay_causes/`
- Glue: `lake.orr_operator_delay_causes`
- Format: Parquet (ParquetHiveSerDe)
- Not partitioned (small table, 34,725 rows on 2026-11-05)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| financial_year_start | int | No | Key | Start year of the rail financial year (April to March), e.g. `2026` for 2026-27 |
| rail_period | int | No | Key | Rail period 1–13, roughly four weeks each |
| geography_type | string | No |  | `national`, `sector` or `operator`. National and sector rows are ORR's aggregate rows, e.g. `Great Britain`, `Franchised Long Distance`, `London and South East` |
| operator | string | No | Key | Operator, or the aggregate's name for national and sector rows, as ORR spells it in this table |
| responsibility | string | No | Key | `nr_on_toc`, `toc_on_self` or `toc_on_toc` |
| cause | string | No | Key | `external`, `network management other`, `non track assets`, `severe weather`, `track` (Network Rail); `fleet`, `operations`, `stations`, `toc other`, `traincrew` (operators) |
| delay_minutes | double | No |  | Delay minutes in the period |
| snapshot_date | string | No |  | Date the ORR release this row comes from was loaded |

Key: (`financial_year_start`, `rail_period`, `operator`, `responsibility`, `cause`). Percentages are 0–100.

## Common queries

```sql
select responsibility, cause, sum(delay_minutes) as delay_minutes
from lake.orr_operator_delay_causes
where operator = 'Great Western Railway' and financial_year_start = 2026
group by 1, 2 order by 3 desc
```

## Source ETL / code

`rail_data_model/sql/orr_operator_delay_causes.sql` in the [rail-etl repo](https://github.com/DanteLore/rail-etl), run by the `model_rail_data` Lambda. It reads only the latest snapshot of each source table, which holds the full history including ORR's revisions, and pivots ORR's one-row-per-cell incoming layout into columns. Cells with no number (`[z]` or empty) are null.

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

- Delay minutes aren't normalised for size; compare operators with `orr_network_rail_performance` (per 1,000 train miles) or as shares of each operator's total.
- Rail periods have no calendar dates in the ORR tables: P1 starts on 1 April and P13 ends on 31 March, so their lengths vary.

## Interested parties

None yet - new dataset.

## Status

Active.
