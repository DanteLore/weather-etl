# lake.bus_positions — Bus positions, Thatcham area

## Description

Every position reported by buses in the Thatcham area: one row per vehicle per reported position, de-duplicated across the minute-by-minute polls of the Bus Open Data Service (BODS) live feed. Each row has the vehicle, operator, line, the journey it is running, its position and when it was recorded, in UTC and UK local time. Rows from one run of one vehicle share a `vehicle_journey_id`. Use it to map where buses went, or for journey times between two points.

Built from [`incoming.bods_vehicle_activity`](../incoming/bods_vehicle_activity.md).

## Classification

Public. Vehicle registrations appear as `vehicle_ref` for some operators; no driver or passenger information.

## Owner

Dan (data platform).

## Source & references

BODS SIRI-VM. See [`incoming.bods_vehicle_activity`](../incoming/bods_vehicle_activity.md).

## License & usage restrictions

Open Government Licence v3.0.

## Location

- S3: `s3://dantelore.data.lake/bus_positions/`
- Glue: `lake.bus_positions`
- Format: Parquet, not partitioned

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| service_date | date | Yes | | Operating day of the journey (`DataFrameRef`) |
| operator_code | string | No | Key | National Operator Code |
| vehicle_ref | string | No | Key | Fleet number or registration |
| line | string | Yes | | Line as published, e.g. `1`, `Community_Connect_B` |
| line_key | string | Yes | | Lower case, underscores as spaces, to match the timetable's line names |
| direction | string | Yes | | `inbound` or `outbound` |
| origin_stop_id, origin_stop_name | string | Yes | | First stop of the journey |
| destination_stop_id, destination_stop_name | string | Yes | | Last stop (name often missing) |
| origin_aimed_departure_local_ts | timestamp | Yes | | Timetabled departure from the first stop, UK local time; only Stagecoach, Thames Travel and West Berkshire send it |
| dated_vehicle_journey_ref | string | Yes | | Operator's journey reference |
| journey_code | string | Yes | | Ticket machine journey code |
| block_ref | string | Yes | | Vehicle working |
| vehicle_journey_id | string | No | | One run of one vehicle: day, operator, vehicle, line, journey reference, end stops and origin time joined with `|` |
| recorded_ts | timestamp | No | Key | When the vehicle recorded the position, UTC |
| recorded_local_ts | timestamp | No | | The same in UK local time |
| first_seen_ts | timestamp | No | | The first poll that included it, UTC |
| report_age_seconds | bigint | No | | How old the position was when first seen. Over a few minutes means a stale position from a vehicle that has stopped reporting |
| latitude, longitude | double | Yes | | Position |
| bearing | double | Yes | | Heading in degrees |

## Common queries

Buses tracked through the area each day, by operator and line:

```sql
select service_date, operator_code, line, count(distinct vehicle_journey_id) as journeys, count(*) as positions
from lake.bus_positions
where report_age_seconds < 300
group by 1, 2, 3 order by 1, 2, 3
```

## Source ETL / code

`rail_data_model/sql/bus_positions.sql` in the [rail-etl repo](https://github.com/DanteLore/rail-etl), rebuilt by the `model_rail_data` Lambda daily at 05:45 UTC.

## Freshness & update cadence

Rebuilt daily; yesterday's positions appear after 05:45 UTC (the raw polls are compacted at 00:20 UTC).

**History from:** 2026-11-12. The first day appears on 2026-11-13.

## Running costs (estimated)

Estimated from the first morning's data (about 800 positions an hour at peak). Method: [rail-etl README](../README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| Athena (nightly rebuild) | 30 × 10 MB minimum; the incoming table grows about 1 MB a day | $0.002 now, about $0.06 after a year |
| S3 storage | About 0.5 MB a day | <$0.001 now, about $0.004 after a year |
| **Total** | | **$0.002 now, about $0.06 after a year** |

## Known issues & caveats

- About one position a minute per bus (the poll interval), not every report the bus makes.
- Stale positions are kept (filter on `report_age_seconds`); a vehicle that went off-line yesterday can appear once with an old timestamp.
- Only inside the area: a journey enters and leaves the table at its edges.
- The same physical journey can get two `vehicle_journey_id`s if the operator changes its references mid-journey.

## Interested parties

Thatcham bus analysis (thatchamplan.co.uk).

## Status

Active.
