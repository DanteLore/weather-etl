# lake.rtt_service_calls — RTT Corridor Station Calls

## Description

One row per public call by a train (or rail replacement bus) at one of the nine tracked stations on the GWR corridor from Newbury to London Paddington. Each row has the scheduled and actual time, lateness, cancellation, platform and number of vehicles. The rows are de-duplicated, with typed timestamps and the station's position along the corridor. Use it for station-level questions ("how late are trains leaving Thatcham in the morning peak?") and for following a delay along a journey. For one row per train, use [`lake.rtt_services`](rtt_services.md).

## Classification

Public. Train running data with no personal or customer information.

## Owner

Dan (data platform).

## Source & references

Derived from [`incoming.rtt_services`](../incoming/rtt_services.md). Original source: [Realtime Trains API](https://api-portal.rtt.io/).

## License & usage restrictions

Realtime Trains free tier: personal and non-commercial use only, under RTT's [Terms of Service](https://www.realtimetrains.co.uk/legal/).

## Location

- S3: `s3://dantelore.data.lake/rtt_service_calls/`
- Glue: `lake.rtt_service_calls`
- Format: Parquet (ParquetHiveSerDe)
- Not partitioned (small table)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| service_id | string | No | Key; join key to `lake.rtt_services` | RTT unique identity, `gb-nr:<train_uid>:<departure date>` |
| train_uid | string | No | | Schedule UID, e.g. `W35101`. The same train in the timetable keeps its UID from day to day |
| departure_date | date | No | | Date the service left its origin |
| station_crs | string | No | Key | Station code: `NBY`, `NRC`, `THA`, `MDG`, `AMT`, `THE`, `RDW`, `RDG` or `PAD` |
| corridor_position | int | No | | Station order from west to east: NBY 1, NRC 2, THA 3, MDG 4, AMT 5, THE 6, RDW 7, RDG 8, PAD 9 |
| corridor_call_number | bigint | No | | Order of this call among the service's tracked calls (1 = first) |
| corridor_calls | bigint | No | | Number of tracked calls the service makes |
| call_type | string | Yes | | `departure`, or `arrival` where the service terminates here. Null for calls loaded before 2026-10-23 |
| operator_code | string | No | | `GW` (Great Western Railway) or `XR` (Elizabeth line) |
| operator_name | string | No | | Operator name |
| is_bus | boolean | No | | Rail replacement bus |
| scheduled_local_ts | timestamp | No | Key | Public (advertised) time of the call, **UK local time** |
| actual_local_ts | timestamp | Yes | | Actual time, UK local time; null if not reported or cancelled |
| lateness_minutes | int | Yes | | Whole minutes late against the public time; negative if early |
| is_cancelled | boolean | No | | The call was cancelled |
| is_on_time | boolean | Yes | | ORR's "on time": less than 1 minute late. False if cancelled; null if no actual was reported |
| number_of_vehicles | int | Yes | | Vehicles (coaches) in the formation, where RTT has it |
| platform | string | Yes | | Actual platform |
| stations_called | string | No | | Comma-separated tracked stations this service called at |
| load_date | string | No | | The `incoming.rtt_services` partition this row came from |

Key: (`service_id`, `station_crs`, `scheduled_local_ts`). A service can call at a station twice, e.g. reversing at Newbury.

## Common queries

Average departure lateness from Thatcham to London by hour, last 4 weeks:

```sql
select hour(c.scheduled_local_ts) as hour, count(*) as calls,
       avg(c.lateness_minutes) as avg_late, avg(cast(c.is_on_time as int)) as on_time_share
from lake.rtt_service_calls c
join lake.rtt_services s using (service_id)
where c.station_crs = 'THA' and s.direction = 'up' and c.departure_date >= current_date - interval '28' day
group by 1 order by 1
```

## Source ETL / code

`rail_data_model/sql/rtt_service_calls.sql` in the [rail-etl repo](https://github.com/DanteLore/rail-etl), run by the `model_rail_data` Lambda.

SQL summary: keeps the most recently written row for each (service, station, scheduled time); drops calls with no public time; parses the timestamps; adds the corridor position and call numbering.

## Freshness & update cadence

Full rebuild daily at 05:45 UTC by `model_rail_data`, after the 04:00 RTT load, which fetches the previous day. So the lake holds data up to yesterday.

**History from:** 2026-07-08. RTT's API keeps only 14 days of history, so nothing earlier can be loaded.

## Running costs (estimated)

Estimated monthly cost in USD at AWS eu-west-1 list prices, from measurements taken 2026-11-05: the S3 size of this table's location, plus the measured rebuild (2.9 MB scanned) on that day. Excludes tax and the AWS free tier (which currently covers all Lambda usage). Method: [rail-etl README](../README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.003 GB, 1 object | <$0.001 |
| Lambda `model_rail_data` (share) | 30 rebuilds × ~5 s × 128 MB | <$0.001 |
| Athena (rebuild) | 30 × 10 MB (the per-query minimum; it scans 2.9 MB of `incoming.rtt_services`) | $0.002 |
| S3 reads (rebuild) | 30 × 117 objects | $0.001 |
| **Total** |  | **$0.003** |

The S3 reads grow by about 30 objects a month, one incoming file per day. Querying it: a full scan reads about 3 MB, roughly $0.00005 per query (Athena bills a 10 MB minimum).

## Known issues & caveats

- **Times are UK local time**, as RTT gives them, not UTC. Convert with `at time zone 'Europe/London'` before joining to UTC tables.
- **Intermediate stations use the departure; a service's last stop uses the arrival.** `call_type` says which (null before 2026-10-23). ORR measures arrivals at every stop, so departure lateness at an intermediate stop is a close proxy, not the same measure.
- Calls with no public time (unadvertised stops, some buses, about 0.5% of incoming rows) are dropped.
- RTT reports lateness in whole minutes, so "under 1 minute late" means a lateness of 0 or less.
- A service is only in incoming if it called at two or more tracked stations on one load day. A service that crosses midnight is split across two daily loads.
- `number_of_vehicles` is RTT's "Know Your Train" data. Null means unknown, not zero.
- 2026-10-25 (the day the clocks went back) was missing until 2026-11-05, because the daily load failed on the 25-hour day. That bug is fixed and the day has been reloaded.

## Interested parties

None yet - new dataset.

## Status

Active.
