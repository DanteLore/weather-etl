# lake.orr_rail_performance — ORR Passenger Rail Performance Statistics

## Description

The national benchmark for rail performance: every number in the 20 ORR passenger rail performance tables, in one long table, from each table's latest release. It covers PPM, cancellations, on-time and "Time to N" punctuality, average passenger lateness, days of severe disruption and delay minutes. Each number has a typed financial year and rail period, plus the entity it describes: national, sector, operator, sub-operator, Network Rail region or Network Rail route. Use it to compare GWR (or the Western route or the Wales and Western region) with other operators and with Great Britain as a whole, period by period.

## Classification

Public. Official statistics with no personal data.

## Owner

Dan (data platform).

## Source & references

Derived from [`incoming.orr_rail_performance`](../incoming/orr_rail_performance.md), which lists every table and its source page. Original source: [ORR Data Portal](https://dataportal.orr.gov.uk/statistics/performance/passenger-rail-performance/).

## License & usage restrictions

Open Government Licence v3.0. Attribution requested: "Office of Rail and Road".

## Location

- S3: `s3://dantelore.data.lake/orr_rail_performance/`
- Glue: `lake.orr_rail_performance`
- Format: Parquet (ParquetHiveSerDe)
- Not partitioned (small table)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| table_id | string | No | Key | ORR table number, e.g. `3124`. Filter on this or on `measure`, because some measures appear in more than one table (e.g. `ppm` in 3167 and `ppm_percentage` in 3114a) |
| table_name | string | No | | ORR table title |
| financial_year_start | int | No | Key | Start year of the rail financial year (April to March), e.g. `2026` for 2026-27 |
| rail_period | int | No | Key | Rail period 1–13, roughly four weeks each |
| geography_type | string | No | | `national`, `sector`, `operator`, `sub_operator`, `nr_region` or `nr_route` |
| geography_name | string | No | Key | What the number describes, e.g. `Great Britain`, `Long Distance`, `Great Western Railway`, `Western` (NR route) or `Wales and Western` (NR region) |
| operator | string | Yes | | Operator name for operator and sub-operator rows (also the aggregate label, e.g. `Great Britain`, in operator-per-row tables) |
| sub_operator | string | Yes | | Sub-operator (3157b, 3167) |
| sector | string | Yes | | Sector (3167) |
| measure | string | No | Key | What is measured, in snake case, e.g. `cancellations_percentage`, `on_time_within_59_seconds`, `time_to_3`, `ppm_percentage`, `delay_minutes_per_1000_train_miles`, `delay_minutes_toc_on_self_fleet` |
| is_moving_annual_average | boolean | No | Key | True for the moving annual average version of a measure, false for the single period |
| value | double | No | | The number. Percentages are 0–100 |
| source_column | string | No | | Source column header, for tracing a value back to ORR's table |
| snapshot_date | string | No | | Date the release this row comes from was loaded |

Key: (`table_id`, `financial_year_start`, `rail_period`, `geography_name`, `sub_operator`, `measure`, `is_moving_annual_average`).

## Common queries

GWR against Great Britain for on time at station stops, latest 13 periods:

```sql
select financial_year_start, rail_period, geography_name, value as on_time_pct
from lake.orr_rail_performance
where table_id = '3138' and measure = 'on_time_within_59_seconds' and not is_moving_annual_average
  and geography_name in ('Great Western Railway', 'Great Britain')
order by financial_year_start desc, rail_period desc, geography_name
limit 26
```

Cancellations by operator for the latest period:

```sql
select geography_name, value as cancellations_pct
from lake.orr_rail_performance
where table_id = '3124' and measure = 'cancellations_percentage' and not is_moving_annual_average
  and financial_year_start * 100 + rail_period = (select max(financial_year_start * 100 + rail_period)
                                                  from lake.orr_rail_performance where table_id = '3124')
order by value desc
```

## Source ETL / code

`rail_data_model/sql/orr_rail_performance.sql` in the [rail-etl repo](https://github.com/DanteLore/rail-etl), run by the `model_rail_data` Lambda.

SQL summary:
- Reads only each table's latest snapshot.
- Parses the two period formats into a year and a period.
- In wide tables, takes the entity from the column name: regions and routes are title-cased so they're spelled the same in every table, and the `National` / `Sector` / `Franchised` / `Open Access` prefixes are stripped.
- Puts aggregate rows in operator columns (e.g. `Great Britain`, `Franchised Long Distance`) under `national` or `sector`.
- Drops cells with no number (`[z]` or empty).

## Freshness & update cadence

Full rebuild daily at 05:45 UTC by `model_rail_data`, after the 05:15 ORR load. The data itself changes about every four weeks, when ORR publishes a period.

**History from:** 2010-11 P01 (3144a/b), 2014-15 P01 (3138), 2019-20 P01 (the other tables).

## Running costs (estimated)

Estimated monthly cost in USD at AWS eu-west-1 list prices, from measurements taken 2026-11-05: the S3 size of this table's location, plus the measured rebuild (0.9 MB scanned) on that day. Excludes tax and the AWS free tier (which currently covers all Lambda usage). Method: [rail-etl README](../README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.0007 GB, 1 object | <$0.001 |
| Lambda `model_rail_data` (share) | 30 rebuilds × ~5 s × 128 MB | <$0.001 |
| Athena (rebuild) | 30 × 10 MB (the per-query minimum; it scans 0.9 MB of `incoming.orr_rail_performance`) | $0.002 |
| S3 reads (rebuild) | 30 × ~20 objects | <$0.001 |
| **Total** |  | **$0.002** |

Querying it: a full scan reads under 1 MB, roughly $0.00005 per query (Athena bills a 10 MB minimum).

## Known issues & caveats

- **3182a is derived, not as published.** ORR's table 3182a (total delay minutes per 1,000 train miles by route) repeats 3182c (operator-attributable) exactly, in every cell checked on 2026-11-05. By region, 3181a equals 3181b + 3181c to within rounding in all 920 cells, so the lake sets 3182a to 3182b + 3182c, rounded to 0.1. `incoming.orr_rail_performance` keeps the page as published.
- **ORR's "on time" counts arrivals within 59 seconds at each recorded station stop.** `lake.rtt_services.is_on_time` uses the closest match RTT allows (lateness under 1 minute, in RTT's whole minutes), so expect small differences.
- Operator names differ between tables and over time (e.g. `Lumo (East Coast)` / `Lumo East Coast`, `Govia Thameslink Railway` / `Greater Thameslink Railway`). Check `geography_name` values before joining tables.
- `Franchised National`, `Franchised England & Wales` and `Total England & Wales` are `national` rows, but they cover a subset of operators. They aren't the same as `Great Britain`.
- Table 3167 has stopped at 2025-26 P13, because ORR has paused it.
- No calendar dates: rail periods don't map to fixed calendar dates (P1 starts on 1 April and P13 ends on 31 March, so their lengths vary), and the ORR tables give no period dates.

## Interested parties

None yet - new dataset.

## Status

Active.
