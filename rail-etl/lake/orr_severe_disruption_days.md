# lake.orr_severe_disruption_days — ORR Days of Severe Disruption

## Description

The number of days in each rail period on which performance was severely disrupted, for Great Britain and for each sub-operator. Use it to spot bad periods and to exclude extreme days when comparing performance.

## Classification

Public. Official statistics with no personal data.

## Owner

Dan (data platform).

## Source & references

ORR table 3157a and 3157b: *Days of severe disruption (Great Britain, and by sub-operator)*. Derived from [`incoming.orr_rail_performance`](../incoming/orr_rail_performance.md), which links to every source page. Original source: [ORR Data Portal](https://dataportal.orr.gov.uk/statistics/performance/passenger-rail-performance/).

## License & usage restrictions

Open Government Licence v3.0. Attribution requested: "Office of Rail and Road".

## Location

- S3: `s3://dantelore.data.lake/orr_severe_disruption_days/`
- Glue: `lake.orr_severe_disruption_days`
- Format: Parquet (ParquetHiveSerDe)
- Not partitioned (small table, 2,206 rows on 2026-11-05)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| financial_year_start | int | No | Key | Start year of the rail financial year (April to March), e.g. `2026` for 2026-27 |
| rail_period | int | No | Key | Rail period 1–13, roughly four weeks each |
| geography_type | string | No | Key | `national` (3157a) or `sub_operator` (3157b) |
| operator | string | No | Key | `Great Britain`, or the sub-operator's operator |
| sub_operator | string | Yes | Key | Sub-operator; null for Great Britain |
| severely_disrupted_days | int | No |  | Days of severe disruption in the period |
| snapshot_date | string | No |  | Date the ORR release this row comes from was loaded |

Key: (`financial_year_start`, `rail_period`, `operator`, `sub_operator`). Percentages are 0–100.

## Common queries

```sql
select financial_year_start, rail_period, sub_operator, severely_disrupted_days
from lake.orr_severe_disruption_days
where operator = 'Great Western Railway'
order by financial_year_start desc, rail_period desc
```

## Source ETL / code

`rail_data_model/sql/orr_severe_disruption_days.sql` in the [rail-etl repo](https://github.com/DanteLore/rail-etl), run by the `model_rail_data` Lambda. It reads only the latest snapshot of each source table, which holds the full history including ORR's revisions, and pivots ORR's one-row-per-cell incoming layout into columns. Cells with no number (`[z]` or empty) are null.

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

- Only periods with at least one severely disrupted day have a row (the smallest value is 1), so a missing period most likely means zero.
- ORR's definition of a severely disrupted day is in its quality and methodology report on the source page.
- Rail periods have no calendar dates in the ORR tables: P1 starts on 1 April and P13 ends on 31 March, so their lengths vary.

## Interested parties

None yet - new dataset.

## Status

Active.
