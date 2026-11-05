# lake.orr_operator_cancellations — ORR Cancellations by Operator

## Description

Trains planned and cancelled per four-week rail period for every operator, plus Great Britain and sector totals. Cancellations are scored by who was responsible. Use it to compare GWR's cancellation rate with other operators and with the national average.

## Classification

Public. Official statistics with no personal data.

## Owner

Dan (data platform).

## Source & references

ORR table 3124: *Trains planned and cancellations by operator (periodic)*. Derived from [`incoming.orr_rail_performance`](../incoming/orr_rail_performance.md), which links to every source page. Original source: [ORR Data Portal](https://dataportal.orr.gov.uk/statistics/performance/passenger-rail-performance/).

## License & usage restrictions

Open Government Licence v3.0. Attribution requested: "Office of Rail and Road".

## Location

- S3: `s3://dantelore.data.lake/orr_operator_cancellations/`
- Glue: `lake.orr_operator_cancellations`
- Format: Parquet (ParquetHiveSerDe)
- Not partitioned (small table, 2,619 rows on 2026-11-05)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| financial_year_start | int | No | Key | Start year of the rail financial year (April to March), e.g. `2026` for 2026-27 |
| rail_period | int | No | Key | Rail period 1–13, roughly four weeks each |
| geography_type | string | No |  | `national`, `sector` or `operator`. National and sector rows are ORR's aggregate rows, e.g. `Great Britain`, `Franchised Long Distance`, `London and South East` |
| operator | string | No | Key | Operator, or the aggregate's name for national and sector rows, as ORR spells it in this table |
| trains_planned | bigint | Yes |  | Trains planned in the period |
| trains_part_cancelled | bigint | Yes |  | Trains part cancelled (did not run the whole route) |
| trains_full_cancelled | bigint | Yes |  | Trains fully cancelled |
| cancellation_score | double | Yes |  | Cancellations score: full cancellations count 1, part cancellations 0.5 |
| cancellation_score_network_management | double | Yes |  | Score attributed to infrastructure and network management (Network Rail) |
| cancellation_score_infrastructure_external | double | Yes |  | Score attributed to external events affecting the infrastructure owner (e.g. weather, trespass) |
| cancellation_score_operator_fault | double | Yes |  | Score attributed to the train operator |
| cancellation_score_operator_external | double | Yes |  | Score attributed to external events affecting the operator |
| cancellations_pct | double | Yes |  | Cancellation score as a % of trains planned |
| cancellations_pct_maa | double | Yes |  | Cancellation score as a % of trains planned, moving annual average |
| snapshot_date | string | No |  | Date the ORR release this row comes from was loaded |

Key: (`financial_year_start`, `rail_period`, `operator`). Percentages are 0–100.

## Common queries

```sql
select financial_year_start, rail_period, operator, cancellations_pct, cancellations_pct_maa
from lake.orr_operator_cancellations
where operator in ('Great Western Railway', 'Great Britain')
order by financial_year_start desc, rail_period desc, operator
limit 26
```

## Source ETL / code

`rail_data_model/sql/orr_operator_cancellations.sql` in the [rail-etl repo](https://github.com/DanteLore/rail-etl), run by the `model_rail_data` Lambda. It reads only the latest snapshot of each source table, which holds the full history including ORR's revisions, and pivots ORR's one-row-per-cell incoming layout into columns. Cells with no number (`[z]` or empty) are null.

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

- `cancellation_score` counts a part cancellation as half, so it isn't `trains_part_cancelled + trains_full_cancelled`.
- The ORR column headed 'cancellation number' is the cancellation score.
- Rail periods have no calendar dates in the ORR tables: P1 starts on 1 April and P13 ends on 31 March, so their lengths vary.

## Interested parties

None yet - new dataset.

## Status

Active.
