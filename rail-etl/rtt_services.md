# RTT Service Performance (Newbury-Paddington corridor)

## Description

Per-service actual vs scheduled departure/arrival times, cancellation flag and
train formation (vehicle count) for services calling at any of the stations on
the GWR corridor between Newbury and London Paddington: Newbury (NBY), Newbury
Racecourse (NRC), Midgham (MDG), Thatcham (THA), Theale (THE), Aldermaston
(AMT), Reading West (RDW), Reading (RDG) and Paddington (PAD) - filtered to
services that call at more than one of these nine stations. Sourced from the
Realtime Trains (RTT) Next Generation API. Use this to analyse GWR delays,
cancellations and short-formation trains, including delay progression along
the corridor since each station a service calls at gets its own row.

## Classification

Public. Train running data with no personal or customer information.

## Owner

Dan (data platform).

## Source & references

Realtime Trains Next Generation API, `https://data.rtt.io`. Registration via
[api-portal.rtt.io](https://api-portal.rtt.io/), requires an RTT unified login
account. API spec: https://realtimetrains.github.io/api-specification/.

## License & usage restrictions

Free tier is for personal/non-commercial use under RTT's Terms of Service
(https://www.realtimetrains.co.uk/legal/). Commercial or high-volume use
requires contacting RTT support for a separate agreement.

## Location

- S3: `s3://dantelore.data.incoming/rtt/services/`
- Glue: `incoming.rtt_services`
- Format: Parquet, partitioned by `date` (the day the location query was run for)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| unique_identity | string | No | | RTT's unique service identifier, `namespace:identity:departureDate` |
| departure_date | string | No | | Date the service departed its origin, `YYYY-MM-DD` |
| station | string | No | | Station this row's platform/time data was captured at (see STATIONS in rtt_transform.py for the full list) |
| operator_code | string | No | | Two-letter TOC code, e.g. `GW` for Great Western Railway |
| operator_name | string | No | | Full operator name |
| mode_type | string | No | | `TRAIN` or `BUS` (rail replacement) |
| scheduled_time | string | Yes | | Scheduled departure or arrival time (ISO, no timezone) at this station |
| actual_time | string | Yes | | Actual/realtime departure or arrival time, null if not yet reported |
| lateness_minutes | double | Yes | | Lateness in minutes against the advertised timetable, negative if early |
| is_cancelled | boolean | No | | Whether this call was cancelled |
| number_of_vehicles | double | Yes | | Number of vehicles (coaches) in the formation at this station, null if not reported |
| platform | string | Yes | | Actual platform used at this station |
| stations_called | string | No | | Comma-separated subset of tracked stations this service called at (2 or more) |
| date | string | No | Partition key | Date the ETL ran the location query for |

Note: `lateness_minutes` and `number_of_vehicles` are stored as `double`, not
`int`, because pandas/pyarrow promote nullable integer columns to float when
any row is null - the Glue table type must match this or Athena will reject
the partition with a `HIVE_BAD_DATA` schema mismatch error.

One row is emitted per (service, station) pair - a service calling at four of
the tracked stations produces four rows, each with that station's own
scheduled/actual time and delay, all sharing the same `unique_identity` and
`stations_called`. This lets you reconstruct delay progression along the
corridor (e.g. on time at Newbury, +12 min by Reading).

## Source ETL / code

`rtt_etl/rtt_bulk_load.py` (CLI backfill) and `rtt_etl/incoming_lambda/` (daily
scheduled Lambda, `load_rtt_data`, runs 04:00 UTC) in this repo. Both import
shared fetch/transform logic from `rtt_etl/incoming_lambda/rtt_transform.py`
(station list lives here, as `STATIONS`) and the API client from
`rtt_etl/incoming_lambda/rtt_client.py`, which handles the refresh-token to
access-token exchange.

## Freshness & update cadence

Runs daily via the `load_rtt_data` Lambda (CloudWatch schedule, 04:00 UTC),
pulling the previous day's data. **Important limitation:** RTT's Next
Generation API restricts history depth per account (14 days on a standard
token, check `/api/info`'s `restrictHistoryDays`) - there is no way to backfill
further back than that window. History accumulates going forward from when
this ETL first ran (2026-07-19), it is not a historical archive.

## Running costs (estimated)

Estimated monthly cost in USD at AWS eu-west-1 list prices, from measurements taken 2026-11-05: the S3 size of this table's location, plus the last 30 days of Lambda metrics and Athena query history. Excludes tax and the AWS free tier (which currently covers all Lambda usage). Method: [rail-etl README](README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.003 GB, 116 objects | <$0.001 |
| Lambda `load_rtt_data` | 32 runs × 10.5 s × 128 MB | $0.001 |
| S3 writes | ~32 files + 29 partition-DDL result files | <$0.001 |
| Athena (loader) | 29 `ADD PARTITION` statements — DDL is free | $0 |
| CloudWatch Logs | Lambda log ingestion | <$0.001 |
| **Total** | | **$0.001** |

Querying it: a full scan reads about 0.00 GB, roughly <$0.001 per query (Athena bills a 10 MB minimum).

## Known issues & caveats

- History is capped at ~14 days by the API itself - do not expect a deep
  backfill here, unlike HSP. If a longer history is ever needed, RTT's older
  classic API or HSP would be the source, not this one.
- A service only appears in this table if it calls at more than one of the
  tracked stations - single-station calls are dropped by design.
- `number_of_vehicles` (formation) is only as reliable as RTT's own upstream
  "Know Your Train" data, which is patchy pre-2020 and by operator - treat
  nulls or unexpected values here as "unknown", not "ran empty."
- **`number_of_vehicles` is the only formation signal this dataset can offer.**
  The richer `allocationData` block (per-coach detail, e.g. class of stock)
  exists in the `/gb-nr/service` API schema but returned `null` for every
  service tested (8 real trains, different operators, both plain and
  `detailed=true` calls) on 2026-07-21 - confirmed as an entitlement gate, not
  a per-train data gap, since our token has `entitlements: []` in `/api/info`.
  Would need a different RTT account tier to get more than a vehicle count.
  This matches the gap BRIEF.md already flagged: no source gives a clean,
  comprehensive "planned 9-car, ran as 5-car" flag.
- Account used is restricted to the `gb-nr` namespace only (per
  `namespaceRestriction`/`permittedNamespaces` in `/api/info`) - this is fine
  for GB mainline data but would need a different token for e.g. Ireland.
- No entitlements beyond the base tier were present on the token used to build
  this (`entitlements: []` in `/api/info`) - detailed mode and STP filtering
  were not tested and may not be available.
- The station list grew twice after the initial backfill (Newbury added, then
  five more stations added, both on 2026-07-20). All partitions were dropped
  and the full history reloaded from scratch each time, so there is no
  discontinuity in the data itself, but do not assume the row count per day
  has been stable since this table's creation.
- An earlier version of the ETL had a bug that merged all of a service's
  station calls into a single row, silently keeping only the first station
  seen and discarding the rest - fixed on 2026-07-20 (now one row per
  service/station pair, see Field spec above). Any data loaded before that
  fix would have been missing rows for some stations on multi-stop services;
  the full reload after the fix means this does not affect current data.

## Interested parties

None yet - new dataset.

## Status

Active.
