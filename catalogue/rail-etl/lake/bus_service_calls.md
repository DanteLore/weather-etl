# lake.bus_service_calls — Bus punctuality by stop, Thatcham area

## Description

How punctual the buses in the Thatcham area are: one row per timetabled journey per stop per day, with the timetabled time, the time the bus actually passed, how late it was, and whether the journey was tracked at all. The bus equivalent of [`lake.rtt_service_calls`](rtt_service_calls.md). Covers every stop inside the area (the A4 corridor from Newbury to Theale) on Reading Buses' Jet Black 1, Newbury & District's local routes, Stagecoach's 32, Thames Travel's X24 and X34 and West Berkshire's Community Connect buses. Use it for on-time performance by line, stop or hour, and for timetabled journeys that weren't seen running.

Built from [`lake.bus_planned_calls`](bus_planned_calls.md) and [`lake.bus_positions`](bus_positions.md).

## Classification

Public. No personal data.

## Owner

Dan (data platform).

## Source & references

Bus Open Data Service timetables (GTFS) and live locations (SIRI-VM). See [`incoming.bods_gtfs_*`](../incoming/bods_gtfs.md) and [`incoming.bods_vehicle_activity`](../incoming/bods_vehicle_activity.md). The punctuality window is the Traffic Commissioners' standard: https://www.gov.uk/government/publications/traffic-commissioners-local-bus-services-in-england-and-wales

## License & usage restrictions

Open Government Licence v3.0.

## Location

- S3: `s3://dantelore.data.lake/bus_service_calls/`
- Glue: `lake.bus_service_calls`
- Format: Parquet, not partitioned

## Field spec

The first 23 fields are as in [`lake.bus_planned_calls`](bus_planned_calls.md): `service_date`, `operator_code`, `operator_name`, `line`, `trip_id`, `direction_id`, `trip_headsign`, `origin_stop_id`, `origin_stop_name`, `origin_departure_local_ts`, `destination_stop_id`, `destination_stop_name`, `stop_id`, `stop_name`, `stop_lat`, `stop_lon`, `stop_sequence`, `is_timing_point`, `is_origin`, `is_destination`, `scheduled_arrival_local_ts`, `scheduled_departure_local_ts`, `scheduled_hour`. Then:

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| is_tracked | boolean | No | | True if a bus in the live feed was matched to this journey |
| vehicle_journey_id | string | Yes | | The matched run in `lake.bus_positions` |
| vehicle_ref | string | Yes | | The bus |
| match_method | string | Yes | | `origin time` (the feed gave the journey's timetabled departure, so the match is exact) or `nearest time` (matched on line, end stops and the best fit to the timetable) |
| journey_median_lateness_minutes | double | Yes | | The matched journey's median lateness over the stops it was seen passing |
| actual_local_ts | timestamp | Yes | | When the bus passed the stop, UK local time, interpolated between the positions either side |
| lateness_minutes | double | Yes | | `actual_local_ts` − `scheduled_departure_local_ts`, in minutes (negative is early) |
| pass_detour_metres | double | Yes | | How far the stop is off the straight line between the two positions used: a quality check. Under 20 m is a clean fix |
| is_on_time | boolean | Yes | | No more than 1 minute early and under 6 minutes late (the Traffic Commissioners' window); null if the bus wasn't seen at the stop |

## Common queries

On-time performance by line and day, at stops where the bus was seen:

```sql
select service_date, operator_code, line,
       count(distinct trip_id) as journeys,
       count(distinct case when is_tracked then trip_id end) as journeys_tracked,
       round(avg(case when is_on_time then 1.0 when not is_on_time then 0.0 end) * 100, 1) as pct_stops_on_time,
       round(approx_percentile(lateness_minutes, 0.5), 1) as median_lateness
from lake.bus_service_calls
where scheduled_departure_local_ts < current_timestamp - interval '2' hour
group by 1, 2, 3 order by 1, 2, 3
```

Jet Black 1 lateness at Thatcham Broadway by hour of day:

```sql
select scheduled_hour, count(lateness_minutes) as buses_seen,
       round(avg(lateness_minutes), 1) as avg_lateness,
       round(avg(case when is_on_time then 1.0 else 0.0 end) * 100, 1) as pct_on_time
from lake.bus_service_calls
where operator_code = 'RBUS' and line = '1' and stop_name = 'Thatcham Broadway' and lateness_minutes is not null
group by 1 order by 1
```

In a test on the morning of 2026-11-12 (before the first full day), a Jet Black 1 journey matched to within a minute at all 21 stops it passed. 96% of the line's stop passes were on time.

## Source ETL / code

`rail_data_model/sql/bus_service_calls.sql` in the [rail-etl repo](https://github.com/DanteLore/rail-etl), rebuilt by the `model_rail_data` Lambda daily at 05:45 UTC, after `bus_planned_calls` and `bus_positions` (tables build in name order).

How it works:

1. Each tracked run of a vehicle (`vehicle_journey_id`) gets candidate timetabled journeys with the same day, operator, line, first stop and last stop. If the feed gave the timetabled origin departure, only that journey is a candidate.
2. For each candidate stop, it finds the pair of consecutive positions the stop lies between: the pair where the stop adds the smallest detour to the straight line between them (under 150 m), within 30 minutes before to 2 hours after the timetabled time. The pass time is interpolated by distance along that segment.
3. Each run is matched to the candidate journey that fits best (median lateness closest to zero, with running early counted three times over, since buses run early less often than late), and each journey keeps the run with the most stops seen.

## Freshness & update cadence

Rebuilt daily; yesterday's journeys are complete after 05:45 UTC.

**History from:** 2026-11-12 (live data from 07:42 that day, so earlier journeys show as untracked). The first full day is 2026-11-13.

## Running costs (estimated)

Method: [rail-etl README](../README.md#running-costs). The rebuild scans the two small lake tables, so its cost is Athena's 10 MB minimum.

| Item | Basis | Est. $/month |
|---|---|---|
| Athena (nightly rebuild) | 30 × 10 MB minimum (about 2 MB scanned on the test day), growing with history | $0.002 now, about $0.03 after a year |
| S3 storage | About 0.3 MB a day | <$0.001 now, about $0.003 after a year |
| **Total** | | **$0.002 now, about $0.03 after a year** |

## Known issues & caveats

- **Untracked doesn't mean cancelled.** A journey with `is_tracked = false` either didn't run, ran without its tracker on, or couldn't be matched. Treat untracked journeys as "not seen" and check against the operator's announcements (BODS disruptions, not loaded yet) before calling them cancellations.
- **Matching is inferred for Reading Buses and Newbury & District.** They don't send the timetabled departure time, and the GTFS timetable lacks the ticket machine journey codes they do send. On a frequent route a very late bus can be matched to the next journey instead of its own, which understates lateness. Loading their TransXChange (needs a free BODS API key) would make the match exact.
- **Pass times are estimates.** Positions are about a minute apart; the interpolation assumes steady speed between them, so a bus that waited at the stop is placed mid-wait. Expect errors of up to a minute; use `pass_detour_metres` to drop poor fixes.
- Lateness is measured against the timetabled departure; at the first stop, a bus that arrives early and waits looks early.
- Routes that pass the same stop twice (loops) get one pass per stop, the best-fitting one.
- Only stops inside the area are measured, and buses are only tracked inside it.
- Matching rates by operator haven't been checked over a full day yet; check `journeys_tracked` against `journeys` per line before comparing operators.

## Interested parties

Thatcham bus analysis (thatchamplan.co.uk).

## Status

Active.
