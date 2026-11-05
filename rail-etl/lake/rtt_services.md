# lake.rtt_services — RTT Corridor Services

## Description

One row per train (or rail replacement bus) per day on the GWR corridor from Newbury to London Paddington. It covers every service making public calls at two or more of the nine tracked stations, mostly GWR and the Elizabeth line. Each row gives the train's direction, its first and last tracked station, scheduled and actual times and lateness at both, its worst lateness, and whether it was fully or partly cancelled. It also has on time and PPM flags, the formation, and whether the train ran with fewer coaches than usual. This is the main table for cancellations, delays and short-formed trains between Thatcham, Reading and London.

## Classification

Public. Train running data with no personal or customer information.

## Owner

Dan (data platform).

## Source & references

Derived from [`lake.rtt_service_calls`](rtt_service_calls.md), which comes from [`incoming.rtt_services`](../incoming/rtt_services.md). Original source: [Realtime Trains API](https://api-portal.rtt.io/).

## License & usage restrictions

Realtime Trains free tier: personal and non-commercial use only, under RTT's [Terms of Service](https://www.realtimetrains.co.uk/legal/).

## Location

- S3: `s3://dantelore.data.lake/rtt_services/`
- Glue: `lake.rtt_services`
- Format: Parquet (ParquetHiveSerDe)
- Not partitioned (small table)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| service_id | string | No | Primary; join key to `lake.rtt_service_calls` | RTT unique identity, `gb-nr:<train_uid>:<departure date>` |
| train_uid | string | No | | Schedule UID: the same timetabled train from day to day |
| departure_date | date | No | | Date the service left its origin |
| operator_code | string | No | | `GW` or `XR` |
| operator_name | string | No | | Operator name |
| is_bus | boolean | No | | Rail replacement bus |
| direction | string | Yes | | `up` (towards London) or `down` (away from London), from the first and last tracked stations. Null if both are the same station |
| corridor_origin | string | No | | First tracked station called at |
| corridor_destination | string | No | | Last tracked station called at |
| corridor_calls | bigint | No | | Number of tracked public calls (2 or more) |
| stations_called | string | No | | Comma-separated tracked stations called at |
| scheduled_departure_local_ts | timestamp | No | | Public time at `corridor_origin`, UK local time |
| actual_departure_local_ts | timestamp | Yes | | Actual time at `corridor_origin`, UK local time |
| departure_lateness_minutes | int | Yes | | Lateness at `corridor_origin` (minutes) |
| scheduled_arrival_local_ts | timestamp | No | | Public time at `corridor_destination`, UK local time |
| actual_arrival_local_ts | timestamp | Yes | | Actual time at `corridor_destination`, UK local time |
| arrival_lateness_minutes | int | Yes | | Lateness at `corridor_destination` (minutes) |
| destination_call_type | string | Yes | | `arrival` if the train terminates at `corridor_destination`; `departure` if it carries on beyond the corridor (its lateness is then the departure lateness). Null before 2026-10-23 |
| max_lateness_minutes | int | Yes | | Worst lateness at any tracked call |
| cancelled_calls | bigint | No | | Tracked calls cancelled |
| is_fully_cancelled | boolean | No | | Every tracked call was cancelled |
| is_part_cancelled | boolean | No | | Some, but not all, tracked calls were cancelled |
| is_on_time | boolean | Yes | | Less than 1 minute late at `corridor_destination` (ORR's on-time threshold). False if cancelled there; null if no actual |
| is_ppm | boolean | Yes | | Less than 5 minutes late at `corridor_destination` (the PPM threshold for London & South East and regional services). False if cancelled there; null if no actual |
| min_vehicles | int | Yes | | Fewest vehicles at any tracked call |
| max_vehicles | int | Yes | | Most vehicles at any tracked call |
| typical_vehicles | int | Yes | | Median of `max_vehicles` over every day this `train_uid` ran. Null if fewer than 5 days have a formation |
| is_short_formed | boolean | Yes | | `min_vehicles` is less than `typical_vehicles`: the train ran with fewer coaches than usual. Null when either is unknown |

## Common queries

Daily cancellations and on-time rate for trains between Thatcham and London:

```sql
select departure_date, count(*) as trains,
       count_if(is_fully_cancelled) as cancelled,
       count_if(is_part_cancelled) as part_cancelled,
       avg(cast(is_on_time as int)) as on_time_share,
       count_if(is_short_formed) as short_formed
from lake.rtt_services
where stations_called like '%THA%' and stations_called like '%PAD%'
group by 1 order by 1 desc
```

## Source ETL / code

`rail_data_model/sql/rtt_services.sql` in the [rail-etl repo](https://github.com/DanteLore/rail-etl), run by the `model_rail_data` Lambda after `rtt_service_calls`. Tables are built in name order, and `rtt_service_calls` sorts first.

SQL summary: groups `lake.rtt_service_calls` by service and takes the first and last tracked calls for the origin and destination values. It keeps services with two or more public tracked calls. The typical formation is a window median over each `train_uid` and operator.

## Freshness & update cadence

Full rebuild daily at 05:45 UTC by `model_rail_data`, so the table holds data up to yesterday. `typical_vehicles` and `is_short_formed` can change for past days as more days of a train accumulate.

**History from:** 2026-07-08.

## Running costs (estimated)

Estimated monthly cost in USD at AWS eu-west-1 list prices, from measurements taken 2026-11-05: the S3 size of this table's location, plus the measured rebuild (2.7 MB scanned) on that day. Excludes tax and the AWS free tier (which currently covers all Lambda usage). Method: [rail-etl README](../README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.002 GB, 1 object | <$0.001 |
| Lambda `model_rail_data` (share) | 30 rebuilds × ~5 s × 128 MB | <$0.001 |
| Athena (rebuild) | 30 × 10 MB (the per-query minimum; it scans 2.7 MB of `lake.rtt_service_calls`) | $0.002 |
| S3 reads (rebuild) | 30 × 1 object | <$0.001 |
| **Total** |  | **$0.002** |

Querying it: a full scan reads about 2 MB, roughly $0.00005 per query (Athena bills a 10 MB minimum).

## Known issues & caveats

- **Measured inside the corridor only.** Origin, destination and lateness are at the first and last *tracked* stations. A Paddington to Plymouth train that stops at Newbury has `corridor_destination` Newbury, so its lateness there is a departure lateness (see `destination_call_type`).
- **PPM uses 5 minutes for every service.** Official PPM allows 10 minutes for long-distance services, such as GWR's West of England trains through Newbury, so `is_ppm` is stricter than the official figure for those.
- **Short-formed is relative to the train's own usual formation.** No source gives the planned formation (see BRIEF.md), so a train that always runs short won't be flagged. Formations change at timetable changes; `train_uid` usually changes too, which resets the comparison.
- **No through trains between Thatcham and Paddington on Sundays.** There were none on any of the 17 Sundays loaded: Sunday trains from Newbury run to Reading only. This is the timetable, not missing data.
- Times are UK local time.
- A service whose tracked calls cross midnight can be split between two daily loads, and is then missing if either day has only one tracked call.

## Interested parties

None yet - new dataset.

## Status

Active.
