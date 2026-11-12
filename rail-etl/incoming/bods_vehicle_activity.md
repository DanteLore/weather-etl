# incoming.bods_vehicle_activity — Live bus locations, Thatcham area (BODS SIRI-VM)

## Description

Where every bus in the Thatcham area was, once a minute, from the Bus Open Data Service (BODS) live vehicle location feed (SIRI-VM, also called AVL or real-time bus tracking). One row per vehicle per poll: position, bearing, operator, line, direction, origin and destination stops, and the journey references the operator sends. The area is the A4 corridor from Newbury to Theale (latitude 51.37 to 51.43, longitude −1.36 to −1.10), which takes in Reading Buses' Jet Black 1, Newbury & District (Kennections) local routes, Stagecoach's 32, Thames Travel's X34 and West Berkshire's community buses. History starts on 2026-11-12. Use [`lake.bus_positions`](../lake/bus_positions.md) for de-duplicated positions and [`lake.bus_service_calls`](../lake/bus_service_calls.md) for punctuality.

The raw responses are kept too: each poll's SIRI-VM XML, cut to the vehicles in the area but otherwise unchanged, is saved gzipped under `bods/vehicle_activity_raw/` so the table can be rebuilt.

## Classification

Public. Open government data. Vehicle registrations appear as `vehicle_ref` for some operators; there's no information about drivers or passengers.

## Owner

Dan (data platform).

## Source & references

Department for Transport, Bus Open Data Service: the public all-England SIRI-VM snapshot at `https://data.bus-data.dft.gov.uk/avl/download/bulk_archive` (a zip refreshed about every 30 seconds; no API key needed). Docs: https://data.bus-data.dft.gov.uk/guidance/requirements/ and the SIRI-VM profile https://www.gov.uk/government/publications/technical-guidance-publishing-location-data-using-the-bus-open-data-service-siri-vm

## License & usage restrictions

Open Government Licence v3.0. Attribute DfT / BODS.

## Location

- Raw: `s3://dantelore.data.incoming/bods/vehicle_activity_raw/received_date=<YYYY-MM-DD>/bods_vm_<YYYYMMDDTHHMMSS>Z.xml.gz` (one file per poll; not a Glue table)
- S3: `s3://dantelore.data.incoming/bods/vehicle_activity/received_date=<YYYY-MM-DD>/bods_vehicle_activity_<YYYY-MM-DD>.parquet`
- Glue: `incoming.bods_vehicle_activity`
- Format: Parquet, one file per day, partitioned by `received_date` (UTC date of the poll)

## Field spec

Times are UTC. Fields are the SIRI-VM elements of each `VehicleActivity`, snake_cased; any can be missing if the operator doesn't send it.

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| response_timestamp | timestamp | No | | When BODS produced the snapshot |
| recorded_at_time | timestamp | Yes | Key (with vehicle_ref) | When the vehicle reported this position |
| item_identifier | string | Yes | | BODS ID for this vehicle activity |
| valid_until_time | timestamp | Yes | | |
| line_ref | string | Yes | | Line, e.g. `1`, `1a`, `Community_Connect_B` |
| direction_ref | string | Yes | | `inbound` or `outbound` |
| data_frame_ref | string | Yes | | Operating day of the journey, `YYYY-MM-DD` |
| dated_vehicle_journey_ref | string | Yes | | Operator's journey reference for the day (often a small number) |
| published_line_name | string | Yes | | Line as shown to the public |
| operator_ref | string | Yes | | National Operator Code: `RBUS` Reading Buses, `NADS` Newbury & District, `SCSO` Stagecoach South, `THTR` Thames Travel, `WBCT` West Berkshire Council |
| origin_ref, origin_name | string | Yes | | First stop (NaPTAN ATCO code) and name |
| destination_ref, destination_name | string | Yes | | Last stop |
| origin_aimed_departure_time | timestamp | Yes | | Timetabled departure from the first stop. Sent by Stagecoach, Thames Travel and West Berkshire; not by Reading Buses or Newbury & District |
| destination_aimed_arrival_time | timestamp | Yes | | Timetabled arrival at the last stop |
| longitude, latitude | double | Yes | | Position (WGS84) |
| bearing | double | Yes | | Heading in degrees |
| block_ref | string | Yes | | Operator's vehicle working |
| vehicle_journey_ref | string | Yes | | Operator's journey code (Stagecoach) |
| vehicle_ref | string | Yes | | Fleet number or registration |
| ticket_machine_service_code | string | Yes | | Service code on the ticket machine |
| journey_code | string | Yes | | Ticket machine journey code |
| vehicle_unique_id | string | Yes | | Operator's vehicle ID |
| source_file | string | No | | The raw file this row came from |
| received_date | string | No | Partition key | UTC date of the poll |

## Source ETL / code

`bods_etl/incoming_lambda/` in the [rail-etl repo](https://github.com/DanteLore/rail-etl):

- `poll_bods_vehicles` Lambda, every minute: downloads the snapshot (about 2.5 MB zipped, 28,000 vehicles), streams through it keeping the vehicles in the area, and saves the cut-down XML.
- `compact_bods_vehicles` Lambda, daily at 00:20 UTC: turns each finished day of raw files into one parquet file and adds the partition. It catches up any missed days.

Parsing is in `bods.py`; tests in `tests/test_bods.py`.

## Freshness & update cadence

Polled every minute, so a bus is seen about once a minute (BODS says vehicles should report every 10 to 30 seconds). Each day's parquet appears shortly after 00:20 UTC the next day; until then the day is only in the raw files.

**History from:** 2026-11-12 07:42 UTC.

## Running costs (estimated)

Estimated monthly cost in USD at AWS eu-west-1 list prices, from the first two hours (2026-11-12): the poller averaged 6.4 s at 512 MB (peak 226 MB used), and raw files are about 2.7 KB each. Method: [rail-etl README](../README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| Lambda `poll_bods_vehicles` | 43,200 runs × 6.4 s × 0.5 GB = 139,000 GB-s, plus requests | $2.33 (covered by the free tier at present) |
| S3 writes | 43,200 raw files | $0.22 |
| CloudWatch Logs | About 30 MB of logs | $0.02 |
| S3 storage | Raw about 120 MB a month, parquet about 30 MB a month | $0.004 now, about $0.04 after a year |
| Lambda `compact_bods_vehicles` | 30 runs, reading 1,440 files each | $0.01 |
| **Total** | | **About $2.60 at list price; about $0.25 while the free tier covers Lambda** |

The cost is almost all the poller downloading the all-England snapshot every minute. With a free BODS API key the poller could ask for just this area (`/api/v1/datafeed/?boundingBox=…`), which would cut its run time to well under a second and the Lambda cost to about $0.05.

## Known issues & caveats

- **One position a minute.** Times when a bus passed a stop have to be interpolated between positions about a minute (up to 800 m) apart.
- **Only buses whose operators publish location data**, and only while the bus's tracker is on. A missing bus is not proof a journey didn't run.
- **Repeated stale positions.** A vehicle that stops reporting stays in the snapshot with its old `recorded_at_time` until its record expires (some have been over 18 hours old). Each poll repeats it; `lake.bus_positions` keeps one row per position and drops ones more than 5 minutes old from matching.
- Vehicles are kept by where they are, not by route: a bus is in the table only while inside the area, and the area covers parts of Newbury and the edge of Reading, not Reading itself.
- Operators don't all send the same fields (see `origin_aimed_departure_time`), which limits matching to the timetable.
- Line names can differ from the timetable's (`Community_Connect_B` here, `Community Connect B` in GTFS).

## Interested parties

Thatcham bus analysis (thatchamplan.co.uk).

## Status

Active.
