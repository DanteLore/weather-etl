# incoming.bods_gtfs_* — Bus timetables, Thatcham area (BODS GTFS)

## Description

The bus timetable for every journey that calls at a stop in the Thatcham area, snapshotted daily from the Bus Open Data Service (BODS). Seven tables, one per GTFS file, cut down to those journeys and everything they reference: operators (agency), routes, trips (journeys), stop times, stops, and calendars of which days each journey runs. Gives the planned journeys that live bus positions are matched against, and bus frequency by stop and hour. Area as for [`incoming.bods_vehicle_activity`](bods_vehicle_activity.md): the A4 corridor from Newbury to Theale. Use [`lake.bus_planned_calls`](../lake/bus_planned_calls.md) for a ready-made day-by-day timetable.

| Table | Rows (2026-11-12) | GTFS file |
|---|---|---|
| `incoming.bods_gtfs_agency` | 7 | Operators, with their National Operator Code |
| `incoming.bods_gtfs_routes` | 35 | Routes (lines) |
| `incoming.bods_gtfs_trips` | 879 | Journeys, with the service (calendar) they run on |
| `incoming.bods_gtfs_stop_times` | 36,006 | Every stop of every journey, including stops outside the area |
| `incoming.bods_gtfs_stops` | 959 | Every stop those journeys call at |
| `incoming.bods_gtfs_calendar` | 27 | Days of the week and date range for each service |
| `incoming.bods_gtfs_calendar_dates` | 681 | Dates added to or removed from a service (bank holidays etc.) |

## Classification

Public. Open government data; no personal data.

## Owner

Dan (data platform).

## Source & references

Department for Transport, Bus Open Data Service: the South East region GTFS file at `https://data.bus-data.dft.gov.uk/timetable/download/gtfs-file/south_east/` (about 250 MB zipped; no API key needed). BODS generates it daily, about 04:40 UTC, from the TransXChange timetables operators publish. GTFS reference: https://gtfs.org/schedule/reference/

## License & usage restrictions

Open Government Licence v3.0. Attribute DfT / BODS.

## Location

- S3: `s3://dantelore.data.incoming/bods/gtfs/<table>/snapshot_date=<YYYY-MM-DD>/<table>_<YYYY-MM-DD>.parquet`, with the feed version in each file's `feed_version` metadata
- Glue: `incoming.bods_gtfs_agency`, `_routes`, `_trips`, `_stop_times`, `_stops`, `_calendar`, `_calendar_dates`
- Format: Parquet, partitioned by `snapshot_date`. Every column is a string, as in the GTFS files; empty values are null

## Field spec

Columns are the GTFS files' own; see the GTFS reference for each. The ones that matter here:

| Table | Field name | Description |
|---|---|---|
| agency | agency_id, agency_name, agency_noc | BODS operator ID (`OP77`), name (`Newbury & District`) and National Operator Code (`NADS`), which matches `operator_ref` in the live feed |
| routes | route_id, agency_id, route_short_name, route_type | Line as the public sees it (`1`, `1a`, `X34`); `route_type` 3 is bus |
| trips | trip_id, route_id, service_id, trip_headsign, direction_id, block_id, vehicle_journey_code | A journey. `service_id` links to the calendar; `vehicle_journey_code` is the TransXChange journey code (not the ticket machine code the live feed sends) |
| stop_times | trip_id, arrival_time, departure_time, stop_id, stop_sequence, pickup_type, drop_off_type, timepoint | Each call. Times are `HH:MM:SS` UK local time and pass `24:00:00` for journeys after midnight; `pickup_type`/`drop_off_type` `1` means no pick-up / set-down; `timepoint` `1` is a timing point |
| stops | stop_id, stop_code, stop_name, stop_lat, stop_lon | `stop_id` is the NaPTAN ATCO code (`030…` West Berkshire), joins to `incoming.naptan_stops.atco_code` |
| calendar | service_id, monday … sunday, start_date, end_date | Runs on days flagged `1` between the dates (`YYYYMMDD`) |
| calendar_dates | service_id, date, exception_type | `1` added on that date, `2` removed |
| all | snapshot_date | Partition key: the UTC date the snapshot was taken |

## Source ETL / code

`load_bods_timetable` Lambda (`bods_etl/incoming_lambda/`, `load_timetable` in `lambda_function.py`, filtering in `bods.py`) in the [rail-etl repo](https://github.com/DanteLore/rail-etl), daily at 05:15 UTC. It finds the stops inside the area, reads `stop_times` twice (to find the journeys calling there, then to take all their calls) and keeps the trips, routes, operators, calendars and stops those journeys use. Shapes and frequencies files aren't loaded.

## Freshness & update cadence

Daily snapshot at 05:15 UTC. Each snapshot holds the timetable as BODS published it that morning, including journeys up to about 10 months ahead.

**History from:** 2026-11-12.

## Running costs (estimated)

From the first run on 2026-11-12: 34 s at 2,048 MB (1,647 MB used), 0.29 MB written. Method: [rail-etl README](../README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| Lambda `load_bods_timetable` | 30 × 34 s × 2 GB | $0.034 |
| S3 storage | About 0.3 MB a day | <$0.001 now, about $0.003 after a year |
| **Total** | | **About $0.04** |

## Known issues & caveats

- **GTFS, not TransXChange.** BODS converts each operator's TransXChange to GTFS. The conversion drops some detail, notably the ticket machine journey codes that would match the live feed exactly for Reading Buses and Newbury & District. Loading TransXChange per operator needs a BODS API key; the all-England TransXChange archive is 1.9 GB a day.
- **Timetables as published, not as run.** Short-notice changes and cancellations aren't here; journeys removed from a later snapshot are the sign of a planned change.
- Only journeys with at least one stop in the area are kept, with all of their stops (so `stop_times` and `stops` reach outside the area).
- Times after midnight are written as `24:xx`, `25:xx`, …, relative to the service day.
- Memory: the Lambda used 1,647 of 2,048 MB on the first run, because `stops` and filtered `stop_times` are held in memory. If BODS's South East file grows a lot, this is the first thing to watch.

## Interested parties

Thatcham bus analysis (thatchamplan.co.uk).

## Status

Active.
