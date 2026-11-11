# lake.nr_schedule_days — Planned Timetable Changes by Day

## Description

One row per passenger train (or replacement bus) per day that calls in the Newbury–Paddington corridor in Network Rail's timetable. Each row has the permanent plan, the plan that actually applied that day, and how it changed: as planned, cancelled through the timetable, cut back (e.g. starting at Reading instead of Paddington), calls removed, overlaid with the same calls, a new schedule, or a bus replacement. Use it to count planned changes that never show as cancellations in RTT or HSP, because the train was taken out of the timetable in advance.

## Classification

Public. Timetable data with no personal information.

## Owner

Dan (data platform).

## Source & references

Derived from [`incoming.nr_schedule_locations`](../incoming/nr_schedule_locations.md). Original source: Network Rail Open Data SCHEDULE feed.

## License & usage restrictions

Open Government Licence. Don't use Network Rail or operator branding.

## Location

- S3: `s3://dantelore.data.lake/nr_schedule_days/`
- Glue: `lake.nr_schedule_days`
- Format: Parquet (ParquetHiveSerDe)
- Not partitioned (small table)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| run_date | date | No | Key | The day the train is planned to run |
| train_uid | string | No | Key | Schedule UID. The same as `train_uid` in `lake.rtt_services`, so join on `train_uid` and `run_date = departure_date` |
| headcode | string | Yes | | Train reporting number, e.g. `2P14` |
| atoc_code | string | Yes | | Operator, e.g. `GW`, `XR` |
| toc_file | string | No | | `EF` (GWR) or `EX` (Elizabeth line) extract |
| has_permanent | boolean | No | | A permanent schedule exists for the day |
| effective_stp | string | No | | The schedule type that applied: `C`, `O`, `N` or `P`, in that order of precedence |
| change_type | string | No | | `as_planned`, `cancelled`, `cut_back`, `calls_removed`, `overlay_same_calls`, `overlay_other_change`, `new` or `bus_replacement` (see below) |
| is_bus_replacement | boolean | No | | The plan that applied is a bus (category `BR`/`BS` or power type `BUS`) |
| permanent_train_category | string | Yes | | Category in the permanent plan, e.g. `OO`, `XX` |
| effective_train_category | string | Yes | | Category in the plan that applied. Null if cancelled |
| effective_power_type | string | Yes | | Power type in the plan that applied |
| permanent_origin_crs / permanent_destination_crs | string | Yes | | Start and end of the permanent plan's whole journey |
| effective_origin_crs / effective_destination_crs | string | Yes | | Start and end of the plan that applied. Null if cancelled |
| permanent_corridor_stations | string | Yes | | Comma-separated public calls in the corridor in the permanent plan, in order |
| effective_corridor_stations | string | Yes | | The same in the plan that applied |
| stations_removed | string | Yes | | Corridor stations in the permanent plan but not the plan that applied |
| snapshot_date | string | No | | The daily snapshot this day was planned from |

`change_type`:
- `as_planned`: the permanent schedule applied.
- `cancelled`: a timetable cancellation (`C`) removed the train.
- `cut_back`: an overlay removed corridor calls and changed its first or last corridor call, e.g. starting at Reading instead of Paddington. Also used where an overlay diverts the train out of the corridor entirely.
- `calls_removed`: an overlay removed corridor calls but kept the same first and last.
- `overlay_same_calls`: an overlay applied with the same corridor calls (usually retiming or platform changes).
- `overlay_other_change`: an overlay added corridor calls.
- `new`: a new schedule with no permanent one, e.g. an extra train.
- `bus_replacement`: the plan that applied is a bus. Its permanent fields are usually empty: buses run as new schedules, while the train they replace shows as `cancelled` under its own UID.

## Common queries

Planned changes for Thatcham trains by day:

```sql
select run_date, change_type, count(*) as trains
from lake.nr_schedule_days
where coalesce(permanent_corridor_stations, effective_corridor_stations) like '%THA%'
group by 1, 2 order by 1, 3 desc
```

## Source ETL / code

`rail_data_model/sql/nr_schedule_days.sql` in the [rail-etl repo](https://github.com/DanteLore/rail-etl), run by the `model_rail_data` Lambda.

SQL summary:
- Each daily snapshot plans the days from its own date to the day before the next snapshot, so each past day keeps the plan as it stood that morning. The latest snapshot also plans the next 4 weeks, so future days change as new snapshots arrive.
- A schedule applies on a day within its dates whose weekday is marked in `days_runs`.
- Precedence is C over O over N over P.
- Only passenger and bus categories are kept (`OO`, `OU`, `OL`, `XX`, `XU`, `XZ`, `XD`, `XI`, `XC`, `BR`, `BS`), and only trains with a corridor call in either plan.

## Freshness & update cadence

Full rebuild daily at 05:45 UTC by `model_rail_data`, after the 05:00 SCHEDULE load. Days up to 4 weeks ahead are included and can change as later snapshots arrive.

**History from:** 2026-11-10 (see caveats).

## Running costs (estimated)

Estimated monthly cost in USD at AWS eu-west-1 list prices, from measurements taken 2026-11-11: the first rebuild (0.8 MB scanned) and table size (0.1 MB). Excludes tax and the AWS free tier (which currently covers all Lambda usage). Method: [rail-etl README](../README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.1 MB, growing with each day | <$0.001 |
| Lambda `model_rail_data` (share) | 30 rebuilds × ~5 s × 128 MB | <$0.001 |
| Athena (rebuild) | 30 × 10 MB minimum now; it scans every snapshot, about 0.5 GB after a year | $0.002 now, about $0.08 after a year |
| S3 reads (rebuild) | 30 × one object per snapshot | <$0.001 now, about $0.004 after a year |
| **Total** |  | **$0.002 now, about $0.09 after a year** |

Querying it: a full scan reads under 1 MB, roughly $0.00005 per query (Athena bills a 10 MB minimum).

## Known issues & caveats

- **No history before 2026-11-10.** The SCHEDULE feed only holds current schedules, so planned changes before then can't be classified. That includes the months behind the analysis's estimate that about 10% of direct London runs were changed through the timetable.
- **Weekend engineering work dominates.** In the first 4 weeks, most `cut_back` days are weekends with Paddington closed, when trains start or end at Reading, Maidenhead or Swindon. On weekdays 16–20 November, 15 trains were cut back.
- **Same-day changes aren't included** (VSTP isn't loaded), and nor are on-the-day cancellations; use RTT or HSP for those.
- **Bank holidays aren't applied.** `bank_holiday_running` is null in every schedule so far; if it's ever set, those trains may show on bank holidays they don't run.
- **Joins and divisions** (associations) aren't modelled.

## Interested parties

The Thatcham rail page analysis.

## Status

Active.
