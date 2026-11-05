# lake.orr_sub_operator_performance — ORR Performance by Sub-Operator

## Description

Right Time, PPM and CaSL per rail period for each operator's sub-operators (service groups; GWR's are `London and Thames Valley`, `West` and `High Speed`), with operator, sector and national totals. The finest breakdown ORR publishes, so it's the closest national comparison for a single route.

## Classification

Public. Official statistics with no personal data.

## Owner

Dan (data platform).

## Source & references

ORR table 3167: *Disaggregated train punctuality and reliability performance by sub-operator (periodic)*. Derived from [`incoming.orr_rail_performance`](../incoming/orr_rail_performance.md), which links to every source page. Original source: [ORR Data Portal](https://dataportal.orr.gov.uk/statistics/performance/passenger-rail-performance/).

## License & usage restrictions

Open Government Licence v3.0. Attribution requested: "Office of Rail and Road".

## Location

- S3: `s3://dantelore.data.lake/orr_sub_operator_performance/`
- Glue: `lake.orr_sub_operator_performance`
- Format: Parquet (ParquetHiveSerDe)
- Not partitioned (small table, 8,647 rows on 2026-11-05)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| financial_year_start | int | No | Key | Start year of the rail financial year (April to March), e.g. `2026` for 2026-27 |
| rail_period | int | No | Key | Rail period 1–13, roughly four weeks each |
| geography_type | string | No |  | `sub_operator` where `sub_operator` is set; otherwise `operator`, `sector` or `national` |
| operator | string | No | Key | Operator (ORR's `toc name`), or the aggregate's name |
| sub_operator | string | Yes | Key | Sub-operator (service group); null for operator and aggregate rows |
| sector | string | Yes | Key | Sector of the sub-operator |
| trains_planned | bigint | Yes |  | Trains planned |
| right_time_pct | double | Yes |  | % of trains arriving at their destination early or within 59 seconds |
| right_time_pct_maa | double | Yes |  | % of trains arriving at their destination early or within 59 seconds, moving annual average |
| ppm_pct | double | Yes |  | PPM % |
| ppm_pct_maa | double | Yes |  | PPM %, moving annual average |
| casl_pct | double | Yes |  | % of trains cancelled or significantly late (CaSL): 30+ minutes late, or cancelled |
| casl_pct_maa | double | Yes |  | % of trains cancelled or significantly late (CaSL): 30+ minutes late, or cancelled, moving annual average |
| cancellation_score | double | Yes |  | Cancellation score (full = 1, part = 0.5) |
| cancellation_score_maa | double | Yes |  | Cancellation score, moving annual average |
| snapshot_date | string | No |  | Date the ORR release this row comes from was loaded |

Key: (`financial_year_start`, `rail_period`, `operator`, `sub_operator`, `sector`). Percentages are 0–100.

## Common queries

```sql
select financial_year_start, rail_period, sub_operator, ppm_pct, casl_pct
from lake.orr_sub_operator_performance
where operator = 'Great Western Railway' and sub_operator is not null
order by financial_year_start desc, rail_period desc, sub_operator
```

## Source ETL / code

`rail_data_model/sql/orr_sub_operator_performance.sql` in the [rail-etl repo](https://github.com/DanteLore/rail-etl), run by the `model_rail_data` Lambda. It reads only the latest snapshot of each source table, which holds the full history including ORR's revisions, and pivots ORR's one-row-per-cell incoming layout into columns. Cells with no number (`[z]` or empty) are null.

## Freshness & update cadence

Full rebuild daily at 05:45 UTC by `model_rail_data`, after the 05:15 ORR load. The data itself changes about every four weeks, when ORR publishes a rail period (3–4 weeks after it ends).

**History from:** 2019-20 P01, **to 2025-26 P13**.

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

- **ORR has paused this table** ("we have paused updates to Table 3167 whilst we review its content"), so it ends at 2025-26 P13.
- Right Time and PPM here are destination measures, unlike `orr_operator_punctuality`, which measures every station stop.
- Rail periods have no calendar dates in the ORR tables: P1 starts on 1 April and P13 ends on 31 March, so their lengths vary.

## Interested parties

None yet - new dataset.

## Status

Active.
