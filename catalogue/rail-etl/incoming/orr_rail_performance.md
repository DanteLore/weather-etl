# incoming.orr_rail_performance — ORR Passenger Rail Performance Tables

## Description

Every passenger rail performance table that the Office of Rail and Road (ORR) Data Portal publishes as data, 20 tables in all. They cover PPM, cancellations, on-time and "Time to N" punctuality at station stops, average passenger lateness, days of severe disruption and delay minutes by cause. Each table is broken down by operator, sector, Network Rail region or route, or sub-operator, for every four-week rail period from 2010, 2014 or 2019, depending on the table. This is the national benchmark for comparing the corridor data in `rtt_services`.

Each table is stored as one row per source cell, so tables with different shapes share one schema. Use [`lake.orr_rail_performance`](../lake/orr_rail_performance.md) for analysis: it parses the periods, entities and measures and keeps only the latest release.

## Classification

Public. Official statistics with no personal data.

## Owner

Dan (data platform).

## Source & references

ORR Data Portal, [passenger rail performance](https://dataportal.orr.gov.uk/statistics/performance/passenger-rail-performance/). Each table page, e.g. [Table 3124](https://dataportal.orr.gov.uk/statistics/performance/passenger-rail-performance/table-3124-trains-planned-and-cancellations-by-operator-periodic/), embeds its full history as an HTML table, which the page's Download CSV/XLSX buttons export in the browser. There is no API, so the loader parses that HTML table.

Tables loaded (id: subject, breakdown, history from):

| Table | Subject | Breakdown | From |
|---|---|---|---|
| 3114a / 3114b | PPM, periodic / moving annual average | National, sector, operator (wide) | 2019-20 |
| 3121 / 3122 | Cancellations % | NR region / NR route (wide) | 2019-20 |
| 3124 | Trains planned, part/full cancelled, cancellation score by responsibility | National, operator | 2019-20 |
| 3131 / 3132 | On time at recorded station stops % | NR region / NR route (wide) | 2019-20 |
| 3138 | Punctuality at recorded station stops: early, on time, Time to 3…30, 15+/20+/30+ minutes late | National, operator | 2014-15 |
| 3144a / 3144b | Average passenger lateness (minutes), periodic / moving annual average | National, sector, operator (wide) | 2010-11 |
| 3157a / 3157b | Days of severe disruption | GB / sub-operator | 2019-20 |
| 3167 | Right Time, PPM, CaSL, cancellation score | Operator, sub-operator, sector | 2019-20, to 2025-26 P13 |
| 3181a/b/c | Delay minutes per 1,000 train miles: total / NR-attributable / operator-attributable | NR region (wide) | 2019-20 |
| 3182a/b/c | As 3181, by NR route | NR route (wide) | 2019-20 |
| 3184 | Delay minutes by operator and cause (NR on TOC, TOC on self, TOC on TOC) | Operator | 2019-20 |

Not loaded: tables 3103, 3113, 3123 and 3133 are only offered as archived ODS files. 3133, the station punctuality table named in BRIEF.md, is replaced by 3138.

## License & usage restrictions

Open Government Licence v3.0. Free to reuse, including commercially. Attribution requested: "Office of Rail and Road".

## Location

- S3: `s3://dantelore.data.incoming/orr/rail_performance/table_id=<id>/snapshot_date=<YYYY-MM-DD>/orr_<id>_<YYYY-MM-DD>.parquet`
- Glue: `incoming.orr_rail_performance`
- Format: Parquet, partitioned by `table_id` and `snapshot_date`

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| table_name | string | No | | Page title, e.g. `Table 3124 - Trains planned and cancellations by operator (periodic)` |
| row_number | bigint | No | Key (with table_id, snapshot_date, column_name) | 1-based row number in the source table |
| time_period | string | No | | Rail period as ORR writes it: `Apr 2026 to Mar 2027 (Period 07)` or, in 3144a/b, `2026-27 Period 07` |
| operator | string | Yes | | The operator column of an operator-per-row table (`Operator`, `national or operator`, `toc name`, `train operating company name`). Includes aggregate rows such as `Great Britain` and `Franchised Long Distance`. Null in wide tables |
| sub_operator | string | Yes | | `Sub operator` / `Sub-Operator` column (3157b, 3167) |
| sector | string | Yes | | `Sector` column (3167) |
| column_name | string | No | Key | Source column header. In wide tables this names the entity as well as the measure, e.g. `wales and western periodic` or `Franchised Great Western Railway` |
| value | string | Yes | | Cell text as published: a number, `[z]` (not applicable) or null (empty cell) |
| table_id | string | No | Partition key | ORR table number, e.g. `3124`, `3181b` |
| snapshot_date | string | No | Partition key | UTC date the loader stored this release |

## Source ETL / code

`orr_etl/incoming_lambda/` in the [rail-etl repo](https://github.com/DanteLore/rail-etl). It runs as the `load_orr_data` Lambda daily at 05:15 UTC. `orr_tables.py` holds the table list (`TABLES`), the HTML parser and the flattening; `lambda_function.py` stores the snapshots. Tests are in `tests/test_orr_tables.py`. Invoke with `{"tables": ["3124"]}` to load only some tables.

## Freshness & update cadence

ORR publishes each rail period about 3 to 4 weeks after the period ends (13 periods a year) and can revise earlier periods. The Lambda checks every table daily and stores a new snapshot only when the table's content has changed. A SHA-256 hash of the parsed rows is kept in the S3 object metadata (`content-sha256`), so this table gains about 13 snapshots a year per table. Every snapshot holds the table's full history, so the latest snapshot supersedes the earlier ones; they're kept as a record of revisions.

**History from:** first loaded 2026-11-05, with history in the data from 2010-11 P01 (3144a/b), 2014-15 P01 (3138) or 2019-20 P01 (the rest). Latest period at first load: 2026-27 P07.

## Running costs (estimated)

Estimated monthly cost in USD at AWS eu-west-1 list prices, from measurements taken 2026-11-05: the S3 size of this table's location, plus the two `load_orr_data` runs made on that day (40 s each, 226 MB peak). Excludes tax and the AWS free tier (which currently covers all Lambda usage). Method: [rail-etl README](../README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.001 GB, 20 objects; grows by about 1 MB per ORR release (about 13 a year) | <$0.001 |
| Lambda `load_orr_data` | 30 runs × 40 s × 512 MB | $0.010 |
| S3 requests (loader) | 30 × (20 LIST + 20 HEAD), plus about 20 PUTs per ORR release | $0.003 |
| Athena (loader) | 20 `ADD PARTITION` statements per release — DDL is free | $0 |
| CloudWatch Logs | Lambda log ingestion | <$0.001 |
| **Total** |  | **$0.01** |

Querying it: a full scan reads about 1 MB, which Athena bills as its 10 MB minimum, roughly $0.00005 per query.

## Known issues & caveats

- **Page scraping.** If ORR changes the page layout, the parser fails loudly ("No reportTable rows found", or a ragged-row error). Other tables still load and the invocation is marked as failed, so it shows in the Lambda error metric.
- **3167 is paused by ORR.** The page says: "we have paused updates to Table 3167 whilst we review its content". Data stops at 2025-26 P13.
- **3144a/b carry placeholder rows** for future periods of the current year, with every value `[z]`.
- **Operator naming varies between tables**, e.g. `Lumo (East Coast)` in 3124 but `Lumo East Coast` in 3114a. Operators also change over the history (e.g. `Govia Thameslink Railway` and `Greater Thameslink Railway`).
- **3182a and 3182c can have identical values** in some periods (total and operator-attributable delay per 1,000 train miles by route). That is how ORR publishes them, not a load error.
- Snapshots are compared by parsed content, so a cosmetic page change (styling, text above the table) doesn't create a new snapshot.

## Interested parties

None yet - new dataset.

## Status

Active.
