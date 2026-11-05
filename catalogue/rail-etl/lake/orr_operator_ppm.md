# lake.orr_operator_ppm — ORR Public Performance Measure by Operator

## Description

The Public Performance Measure (PPM) per rail period for every operator, the three sectors and Great Britain, England and Wales, and Scotland. PPM is the % of trains arriving at their destination within 5 minutes (London & South East and regional) or 10 minutes (long distance), without being cancelled. It was the headline measure before 2019, and ORR still publishes it.

## Classification

Public. Official statistics with no personal data.

## Owner

Dan (data platform).

## Source & references

ORR table 3114a and 3114b: *Public performance measure by operator and sector (periodic, and moving annual average)*. Derived from [`incoming.orr_rail_performance`](../incoming/orr_rail_performance.md), which links to every source page. Original source: [ORR Data Portal](https://dataportal.orr.gov.uk/statistics/performance/passenger-rail-performance/).

## License & usage restrictions

Open Government Licence v3.0. Attribution requested: "Office of Rail and Road".

## Location

- S3: `s3://dantelore.data.lake/orr_operator_ppm/`
- Glue: `lake.orr_operator_ppm`
- Format: Parquet (ParquetHiveSerDe)
- Not partitioned (small table, 2,913 rows on 2026-11-05)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| financial_year_start | int | No | Key | Start year of the rail financial year (April to March), e.g. `2026` for 2026-27 |
| rail_period | int | No | Key | Rail period 1–13, roughly four weeks each |
| geography_type | string | No | Key | `national`, `sector` or `operator` |
| operator | string | No | Key | Operator, sector or national area, with ORR's `National` / `Sector` / `Franchised` / `Open Access` prefix removed |
| operator_type | string | Yes |  | `franchised` or `open_access` for operators, null for national and sector rows |
| ppm_pct | double | Yes |  | PPM % |
| ppm_pct_maa | double | Yes |  | PPM %, moving annual average |
| snapshot_date | string | No |  | Date the ORR release this row comes from was loaded |

Key: (`financial_year_start`, `rail_period`, `geography_type`, `operator`). Percentages are 0–100.

## Common queries

```sql
select financial_year_start, rail_period, operator, ppm_pct, ppm_pct_maa
from lake.orr_operator_ppm
where operator in ('Great Western Railway', 'Great Britain')
order by financial_year_start desc, rail_period desc, operator
limit 26
```

## Source ETL / code

`rail_data_model/sql/orr_operator_ppm.sql` in the [rail-etl repo](https://github.com/DanteLore/rail-etl), run by the `model_rail_data` Lambda. It reads only the latest snapshot of each source table, which holds the full history including ORR's revisions, and pivots ORR's one-row-per-cell incoming layout into columns. Cells with no number (`[z]` or empty) are null.

## Freshness & update cadence

Full rebuild daily at 05:45 UTC by `model_rail_data`, after the 05:15 ORR load. The data itself changes about every four weeks, when ORR publishes a rail period (3–4 weeks after it ends).

**History from:** 2019-20 P01 (moving annual average from P13).

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

- Built from two ORR tables with one column per operator. The operator comes from the column header.
- Operator names follow these tables, e.g. `Lumo East Coast` here but `Lumo (East Coast)` in `orr_operator_cancellations`.
- Rail periods have no calendar dates in the ORR tables: P1 starts on 1 April and P13 ends on 31 March, so their lengths vary.

## Interested parties

None yet - new dataset.

## Status

Active.
