# incoming.nr_schedule_locations — Network Rail Timetable (SCHEDULE) for the corridor

## Description

The working timetable for every GWR and Elizabeth line train that calls in the Newbury–Paddington corridor on any of its schedules. It includes short-term planned (STP) changes: overlays that replace the permanent schedule on some dates (e.g. cut back to Reading for engineering work), cancellations that remove it, new schedules and bus replacements. One row per stop of each schedule, with a new snapshot each day. Use [`lake.nr_schedule_days`](../lake/nr_schedule_days.md) for analysis: it works out which schedule applied on each day and how it differed from the permanent plan.

## Classification

Public. Timetable data with no personal information.

## Owner

Dan (data platform).

## Source & references

Network Rail Open Data, SCHEDULE feed: the daily full extract per operator, `CIF_EF_TOC_FULL_DAILY` (GWR) and `CIF_EX_TOC_FULL_DAILY` (Elizabeth line), from `https://publicdatafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate`. Gzipped JSON lines, about 9 MB and 5 MB. The login is a Network Rail open data account, stored in Parameter Store as `networkrailusername` and `networkrailpassword` (us-west-2) and bundled in the git-ignored `api_keys.py`. Format: [Open Rail Data wiki: SCHEDULE](https://wiki.openraildata.com/index.php?title=SCHEDULE).

## License & usage restrictions

Open Government Licence. Don't use Network Rail or operator branding.

## Location

- S3: `s3://dantelore.data.incoming/nr/schedule_locations/snapshot_date=<YYYY-MM-DD>/nr_schedule_locations_<YYYY-MM-DD>.parquet`
- Glue: `incoming.nr_schedule_locations`
- Format: Parquet, partitioned by `snapshot_date`

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| toc_file | string | No | | Which extract: `EF` (GWR) or `EX` (Elizabeth line) |
| extract_timestamp | string | No | | When Network Rail produced the extract (UTC, ISO 8601), about 00:30 each day |
| train_uid | string | No | Key | Schedule UID, e.g. `C48543`. The same as RTT's `train_uid` |
| stp_indicator | string | No | Key | `P` permanent, `O` overlay (replaces P on its dates), `N` new (no P) or `C` cancellation (removes P on its dates) |
| schedule_start_date | string | No | Key | First date the schedule applies, `YYYY-MM-DD` |
| schedule_end_date | string | No | | Last date it applies |
| days_runs | string | No | | Seven `0`/`1` characters, Monday first, e.g. `1111100` for weekdays |
| bank_holiday_running | string | Yes | | Bank holiday code; null in every schedule loaded so far |
| train_status | string | Yes | | Network Rail train status code, e.g. `P` |
| atoc_code | string | Yes | | Operator, e.g. `GW`, `XR`. Null for cancellations |
| train_category | string | Yes | | E.g. `OO` ordinary passenger, `XX` express, `XZ` sleeper, `BR` bus replacement, `EE` empty coaches. Null for cancellations |
| power_type | string | Yes | | E.g. `EMU`, `DMU`, `D`. Null for buses and cancellations |
| headcode | string | Yes | | Train reporting number, e.g. `2P14` |
| location_order | bigint | Yes | Key | The stop's position in the schedule, from 1. Null for cancellations, which list no stops |
| tiploc | string | Yes | | Network Rail location code, e.g. `THATCHM` |
| crs | string | Yes | | Station CRS code from the extract's location records, e.g. `THA`. Null for locations that aren't stations |
| location_type | string | Yes | | `LO` origin, `LI` intermediate, `LT` destination |
| public_arrival | string | Yes | | Public timetable arrival, `HHMM`. Null where the stop isn't in the public timetable |
| public_departure | string | Yes | | Public timetable departure, `HHMM` |
| arrival | string | Yes | | Working timetable arrival, `HHMM` or `HHMMH` (H = half minute) |
| departure | string | Yes | | Working timetable departure |
| platform | string | Yes | | Planned platform |
| snapshot_date | string | No | Partition key | UTC date of `extract_timestamp` |

## Source ETL / code

`nr_schedule_etl/incoming_lambda/` in the [rail-etl repo](https://github.com/DanteLore/rail-etl), running as the `load_nr_schedule_data` Lambda daily at 05:00 UTC. `nr_schedule.py` defines `CORRIDOR_CRS` and the schema, and filters the extracts. Tests are in `tests/test_nr_schedule.py`.

How the filter works: a first pass finds every train UID with a schedule that stops in the corridor. A second pass keeps all of those UIDs' schedules, including overlays that divert the train away from the corridor, and cancellations, which list no stops. Passing points aren't stored.

## Freshness & update cadence

Network Rail produces each full extract at about 00:30 UTC. The Lambda loads both at 05:00 UTC and names the snapshot after the extract's date, so a rerun on the same day overwrites rather than duplicating it.

**History from:** 2026-11-10. The SCHEDULE feed has no history: each extract holds only schedules that haven't ended yet, so timetable changes before 2026-11-10 can't be loaded.

## Running costs (estimated)

Estimated monthly cost in USD at AWS eu-west-1 list prices, from measurements taken 2026-11-11: the first snapshot (1.4 MB, 168,948 rows) and the first Lambda run (23 s, 403 MB peak). Excludes tax and the AWS free tier (which currently covers all Lambda usage). Method: [rail-etl README](../README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | About 1.4 MB a day, so about 0.5 GB a year | $0.001 now, about $0.012 after a year |
| Lambda `load_nr_schedule_data` | 30 runs × 23 s × 1,024 MB | $0.012 |
| S3 writes | 30 files plus Athena result files | <$0.001 |
| Athena (loader) | 30 `ADD PARTITION` statements — DDL is free | $0 |
| CloudWatch Logs | Lambda log ingestion | <$0.001 |
| **Total** |  | **$0.01–0.03** |

Querying it: one snapshot is about 1 MB; a scan of every snapshot grows by about 0.5 GB a year.

## Known issues & caveats

- **No history before 2026-11-10** (see above).
- **Same-day changes aren't here.** Trains added or changed at short notice come through Network Rail's VSTP feed, which is streaming and not loaded.
- **Planned, not actual.** This is what the timetable said would run. Use RTT or HSP for what happened.
- **Associations aren't loaded,** so trains that join or divide show as separate UIDs.

## Interested parties

The Thatcham rail page analysis.

## Status

Active.
