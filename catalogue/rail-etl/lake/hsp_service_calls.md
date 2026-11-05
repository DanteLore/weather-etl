# lake.hsp_service_calls — HSP Train Stops (Newbury line)

## Description

Every stop of every train that called at both Newbury and Reading, across its whole journey, with full timestamps. Each row has the scheduled and actual arrival and departure, lateness, whether the stop was cancelled, and the delay or cancellation reason code. It's the place to trace how a delay built up along a journey, or why a train was cancelled. For one row per train, use [`lake.hsp_services`](hsp_services.md). For coach counts and trains that only run Reading–Paddington, use [`lake.rtt_service_calls`](rtt_service_calls.md).

## Classification

Public. Train running data with no personal or customer information.

## Owner

Dan (data platform).

## Source & references

Derived from [`incoming.hsp_service_calls`](../incoming/hsp_service_calls.md). Original source: National Rail Historic Service Performance API, via the [Rail Data Marketplace](https://raildata.org.uk).

## License & usage restrictions

Open Government Licence v2.0 with National Rail Enquiries amendments. Attribution to National Rail is required.

## Location

- S3: `s3://dantelore.data.lake/hsp_service_calls/`
- Glue: `lake.hsp_service_calls`
- Format: Parquet (ParquetHiveSerDe)
- Not partitioned (small table)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| rid | string | No | Key; join key to `lake.hsp_services` | Darwin train ID for this train on this date |
| service_date | date | No | | Date the train started its journey |
| toc_code | string | No | | Operator code, e.g. `GW` |
| station_crs | string | No | | Station code, including stations outside the corridor |
| stop_number | int | No | Key | Position in the train's journey, from 1 |
| corridor_position | int | Yes | | Position along the Newbury–Paddington corridor, west to east: NBY 1, NRC 2, THA 3, MDG 4, AMT 5, THE 6, RDW 7, RDG 8, PAD 9. Null for other stations |
| scheduled_arrival_local_ts | timestamp | Yes | | Public arrival, **UK local time**. Null at the origin |
| scheduled_departure_local_ts | timestamp | Yes | | Public departure, UK local time. Null at the destination |
| actual_arrival_local_ts | timestamp | Yes | | Actual arrival, UK local time |
| actual_departure_local_ts | timestamp | Yes | | Actual departure, UK local time |
| arrival_lateness_minutes | int | Yes | | Minutes late at arrival; negative if early |
| departure_lateness_minutes | int | Yes | | Minutes late at departure; negative if early |
| reason_code | string | Yes | | Darwin delay or cancellation reason code, e.g. `777` |
| is_cancelled | boolean | No | | The train didn't make this stop: no actual times, and a reason code |
| is_on_time | boolean | Yes | | ORR's "on time": less than 1 minute late at arrival (at departure for the origin). False if cancelled; null if no actual was reported |

## Common queries

How lateness builds up from Newbury to Paddington, by station:

```sql
select station_crs, count(*) as stops,
       avg(coalesce(arrival_lateness_minutes, departure_lateness_minutes)) as avg_late,
       avg(cast(is_on_time as int)) as on_time_share
from lake.hsp_service_calls
where corridor_position is not null
group by station_crs order by min(corridor_position)
```

## Source ETL / code

`rail_data_model/sql/hsp_service_calls.sql` in the [rail-etl repo](https://github.com/DanteLore/rail-etl), run by the `model_rail_data` Lambda.

SQL summary: HSP gives bare `HHMM` times, so the SQL adds a day each time the timetable goes back by more than 12 hours between stops. Lateness takes the shortest way round the clock, so 23:58 to 00:02 is 4 minutes late.

## Freshness & update cadence

Full rebuild daily at 05:45 UTC by `model_rail_data`, after the 04:30 HSP load, so it holds data up to yesterday.

**History from:** filling back to about a year before 2026-11-05 at about one day an hour. The backfill should finish around 2026-11-20. Until then older days are missing, so check `min(service_date)`.

## Running costs (estimated)

Estimated monthly cost in USD at AWS eu-west-1 list prices, from measurements taken 2026-11-05: the rebuild measured on that day, scaled to a year of data. Excludes tax and the AWS free tier (which currently covers all Lambda usage). Method: [rail-etl README](../README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | Under 10 MB once a year is loaded | <$0.001 |
| Lambda `model_rail_data` (share) | 30 rebuilds × ~4 s × 128 MB | <$0.001 |
| Athena (rebuild) | 30 × 10 MB (the per-query minimum; it scans about 7 MB per year of data) | $0.002 |
| S3 reads (rebuild) | 30 × 365 objects | $0.004 |
| **Total** |  | **$0.006** |

Querying it: a full scan reads under 10 MB, roughly $0.00005 per query (Athena bills a 10 MB minimum).

## Known issues & caveats

- **Times are UK local time.** Timestamps are worked out from the clock times, so lateness on the night the clocks change isn't affected, but elapsed time across that change is an hour out.
- **Reason codes have no text yet.** The descriptions are in the Darwin reference data, which isn't loaded.
- **A reason code doesn't always mean a cancellation.** It's also given for delays, on stops where the train still ran.
- **Disagreement with RTT.** On 2026-11-04, RTT marked 8 stops as cancelled at Aldermaston, Midgham and Theale where HSP has actual times. Which is right isn't known.
- Newbury-line trains only; see [`incoming.hsp_service_calls`](../incoming/hsp_service_calls.md).

## Interested parties

None yet - new dataset.

## Status

Active.
