# lake.hsp_services — HSP Trains (Newbury line)

## Description

One row per train per day that called at both Newbury and Reading, across its whole journey. It has the true origin and destination, where the train actually got to, lateness at its destination and at Thatcham, Reading and Paddington, whether it was fully or partly cancelled, on time and PPM flags, and reason codes. With a rolling year of history, this is the main table for long-term cancellations and delays on the Thatcham line. For coach counts, use [`lake.rtt_services`](rtt_services.md).

## Classification

Public. Train running data with no personal or customer information.

## Owner

Dan (data platform).

## Source & references

Derived from [`lake.hsp_service_calls`](hsp_service_calls.md), which comes from [`incoming.hsp_service_calls`](../incoming/hsp_service_calls.md). Original source: National Rail Historic Service Performance API, via the [Rail Data Marketplace](https://raildata.org.uk).

## License & usage restrictions

Open Government Licence v2.0 with National Rail Enquiries amendments. Attribution to National Rail is required.

## Location

- S3: `s3://dantelore.data.lake/hsp_services/`
- Glue: `lake.hsp_services`
- Format: Parquet (ParquetHiveSerDe)
- Not partitioned (small table)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| rid | string | No | Primary; join key to `lake.hsp_service_calls` | Darwin train ID for this train on this date |
| service_date | date | No | | Date the train started its journey |
| toc_code | string | No | | Operator code |
| direction | string | Yes | | `up` (towards London) or `down`, from the train's first and last corridor stations |
| origin_crs | string | No | | First scheduled station of the journey |
| destination_crs | string | No | | Last scheduled station of the journey |
| last_served_crs | string | Yes | | Last station the train actually served. Differs from `destination_crs` if it terminated short. Null if fully cancelled |
| corridor_stations | string | Yes | | Comma-separated corridor stations it was scheduled to call at, in order |
| scheduled_departure_local_ts | timestamp | No | | Public departure from the origin, UK local time |
| scheduled_arrival_local_ts | timestamp | No | | Public arrival at the destination, UK local time |
| actual_departure_local_ts | timestamp | Yes | | Actual departure from the origin |
| actual_arrival_local_ts | timestamp | Yes | | Actual arrival at the destination |
| destination_lateness_minutes | int | Yes | | Minutes late at the destination |
| max_lateness_minutes | int | Yes | | Worst lateness at any stop |
| stops | bigint | No | | Scheduled stops |
| cancelled_stops | bigint | No | | Stops the train didn't make |
| is_fully_cancelled | boolean | No | | Every stop was cancelled |
| is_part_cancelled | boolean | No | | Some, but not all, stops were cancelled |
| is_on_time | boolean | Yes | | Less than 1 minute late at the destination. False if cancelled there |
| is_ppm | boolean | Yes | | Less than 5 minutes late at the destination. False if cancelled there |
| first_reason_code | string | Yes | | First Darwin reason code on the journey, for delay or cancellation |
| cancellation_reason_code | string | Yes | | Reason code at the first cancelled stop |
| tha_lateness_minutes | int | Yes | | Minutes late at Thatcham (arrival, or departure if it starts there). Null if it doesn't call there or was cancelled there |
| rdg_lateness_minutes | int | Yes | | The same at Reading |
| pad_lateness_minutes | int | Yes | | The same at Paddington |
| is_cancelled_at_tha | boolean | No | | Cancelled at Thatcham |
| is_cancelled_at_rdg | boolean | No | | Cancelled at Reading |
| is_cancelled_at_pad | boolean | No | | Cancelled at Paddington |

## Common queries

Thatcham trains by month: how many, how many cancelled at Thatcham, and how many reach Reading on time:

```sql
select date_trunc('month', service_date) as month, count(*) as trains,
       count_if(is_cancelled_at_tha) as cancelled_at_thatcham,
       avg(cast(rdg_lateness_minutes < 1 as int)) as on_time_share_at_reading
from lake.hsp_services
where corridor_stations like '%THA%' and direction = 'up'
group by 1 order by 1
```

## Source ETL / code

`rail_data_model/sql/hsp_services.sql` in the [rail-etl repo](https://github.com/DanteLore/rail-etl), run by the `model_rail_data` Lambda after `hsp_service_calls`. Tables are built in name order, and `hsp_service_calls` sorts first.

## Freshness & update cadence

Full rebuild daily at 05:45 UTC by `model_rail_data`, so it holds data up to yesterday.

**History from:** filling back to about a year before 2026-11-05 at about one day an hour. Lambda 503s are slowing this down (see the incoming table's caveats), so no end date is known. Until it finishes, older days are missing, so check `min(service_date)`.

## Running costs (estimated)

Estimated monthly cost in USD at AWS eu-west-1 list prices, from measurements taken 2026-11-05: the rebuild measured on that day, scaled to a year of data. Excludes tax and the AWS free tier (which currently covers all Lambda usage). Method: [rail-etl README](../README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | Under 10 MB once a year is loaded | <$0.001 |
| Lambda `model_rail_data` (share) | 30 rebuilds × ~4 s × 128 MB | <$0.001 |
| Athena (rebuild) | 30 × 10 MB (the per-query minimum; it scans `lake.hsp_service_calls`) | $0.002 |
| S3 reads (rebuild) | 30 × 1 object | <$0.001 |
| **Total** |  | **$0.002** |

Querying it: a full scan reads under 10 MB, roughly $0.00005 per query (Athena bills a 10 MB minimum).

## Known issues & caveats

- **`is_cancelled` is an inference.** HSP has no cancellation flag, and blank actual times can also mean the train wasn't reported or was diverted. Requiring a reason code as well reduces that risk but doesn't remove it. An experienced Open Rail Data contributor also warned in December 2025 that HSP may not be "sufficiently accurate", and that a train missing from HSP may just not have reached it. Compare with `lake.rtt_services` before relying on exact counts.
- **PPM uses 5 minutes for every train.** Official PPM allows 10 minutes for long-distance services, such as GWR's West of England trains, so `is_ppm` is stricter than the official figure for those.
- **Destination measures are at the true destination**, unlike `lake.rtt_services`, whose are at the last corridor station. A Paddington–Plymouth train's are at Plymouth; use `rdg_lateness_minutes` or `tha_lateness_minutes` for a local view.
- **Reason codes have no text yet.** The descriptions are in the Darwin reference data, which isn't loaded.
- **No coach counts.** HSP has no formation data, so short-formed trains are in `lake.rtt_services` only. To join them, the last five digits of `rid` matched the digits of RTT's `train_uid` for all 97 trains checked on 2026-11-04, on the same date. This isn't documented by either source.
- Newbury-line trains only; see [`incoming.hsp_service_calls`](../incoming/hsp_service_calls.md).

## Interested parties

None yet - new dataset.

## Status

Active.
