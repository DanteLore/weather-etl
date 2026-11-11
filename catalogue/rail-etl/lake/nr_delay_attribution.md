# lake.nr_delay_attribution — Delays and cancellations by cause and responsibility, corridor

## Description

Network Rail's attributed delays and cancellations for trains that start, end or are delayed at one of the nine Newbury–Paddington corridor stations, decoded with Network Rail's glossary. One row per train per incident event. Each row has the train, the event (delay or cancellation, and what kind), where it happened, the cause code and its description, a cause group (e.g. Track, Non-Track Assets, Fleet, Traincrew, External), and who was responsible: Network Rail, the operator, or other. History starts in February 2026, before HSP's first full months and long before RTT's, so it gives the longest view of why corridor trains were cancelled or late.

Built from [`incoming.nr_delay_attribution`](../incoming/nr_delay_attribution.md) and its glossary tables.

## Classification

Public. Operational data with no personal information.

## Owner

Dan (data platform).

## Source & references

Network Rail's historic delay attribution, from the Rail Data Marketplace. See [`incoming.nr_delay_attribution`](../incoming/nr_delay_attribution.md).

## License & usage restrictions

Open Government Licence v3.

## Location

- S3: `s3://dantelore.data.lake/nr_delay_attribution/`
- Glue: `lake.nr_delay_attribution`
- Format: Parquet, not partitioned

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| financial_year_period | string | No | | Railway period, e.g. `2026-27_P05` |
| origin_departure_date | date | No | Key | The train's date |
| trust_train_id | string | No | Key | TRUST train ID, e.g. `741K10MZ26`. Unique within a period, not a year |
| headcode | string | No | | Headcode from the train ID, e.g. `1K10` |
| toc_code | string | No | | Operator, e.g. `EF` GWR, `EX` Elizabeth line, `HM` Heathrow Express, `EH` CrossCountry |
| operator_name | string | Yes | | Operator name from the glossary; null for a few codes it lacks |
| planned_origin_crs, planned_origin_name | string | Yes | | Planned origin station (CRS null for non-station locations) |
| planned_origin_gbtt_ts | timestamp | Yes | | Public timetable departure from origin (UK local time) |
| planned_destination_crs, planned_destination_name | string | Yes | | Planned destination |
| planned_destination_gbtt_ts | timestamp | Yes | | Public timetable arrival at destination |
| service_group_code | string | Yes | | Schedule 8 service group |
| train_service_code | string | Yes | | Train service code |
| is_applicable_timetable | boolean | No | | False for trains not in official performance records (short-term reinstatements) |
| event_type | string | No | | `A`, `M`, `F`, `C`, `S`, `P`, `O` or `D` (see `event_name`) |
| event_group | string | No | | `delay` or `cancellation` |
| event_name | string | No | | automatic, manual, failed to stop, cancelled at origin, scheduled cancellation, cancelled en route, changed origin, diverted |
| cancellation_kind | string | Yes | | `full` (C, S), `part` (P, O: the train ran for only part of its journey), `diverted`, `failed to stop`; null for delays |
| delay_minutes | double | Yes | | Delay minutes for delay events (`A`, `M`); null for cancellations, whose deemed minutes aren't real delay |
| event_start_crs, event_start_name | string | Yes | | Where the delay started (TRUST timing point) |
| event_end_crs, event_end_name | string | Yes | | Where it ended |
| event_ts | timestamp | Yes | | When the train hit the delay |
| is_event_in_corridor | boolean | No | | True if the delay or cancellation happened at a corridor station; false for trains that start or end in the corridor but were delayed elsewhere |
| incident_number | string | No | | TRUST incident number. Several trains share an incident |
| incident_start_ts | timestamp | Yes | | When the incident went live in the system |
| incident_description | string | Yes | | Short internal description |
| incident_reason | string | Yes | | Cause code, e.g. `IB` |
| incident_reason_name | string | Yes | | Short name, e.g. `POINTS FLR` |
| incident_reason_description | string | Yes | | E.g. `Points failure (including no fault found)` |
| incident_category_description | string | Yes | | E.g. `Points failures` |
| cause_group | string | Yes | | Network Rail's JPIP category: Track, Non-Track Assets, Severe Weather, Autumn & Structures, External, Network Management / Other, Fleet, Operations, Traincrew, Stations, TOC Other |
| responsible_manager | string | Yes | | Responsible manager code |
| responsible_organisation_name | string | Yes | | E.g. `Network Rail`, `GWR` |
| responsible_party | string | Yes | | `Network Rail`, `Operator` (any train or freight operator) or `Other`; null for the 0.3% of managers missing from the glossary |
| react_reason | string | Yes | | Reactionary reason code (`Y…`); null for primary delays |
| reactionary_reason_description | string | Yes | | E.g. late inbound train |
| is_reactionary | boolean | No | | True if this is a knock-on from an incident elsewhere |
| attribution_status | string | Yes | | `Attribution Agreed`, `Attribution Disputed` or `Waiting Acceptance` |
| pfpi_minutes | double | Yes | | Raw delay minutes, including deemed minutes on cancellations |
| non_pfpi_minutes | double | Yes | | Delay minutes outside the performance regime |

## Common queries

Cancelled GWR trains to or from Newbury and Bedwyn (the Thatcham stoppers), by cause:

```sql
select responsible_party, cause_group,
       count(distinct trust_train_id || cast(origin_departure_date as varchar)) as cancelled_trains
from lake.nr_delay_attribution
where toc_code = 'EF' and event_group = 'cancellation'
  and (planned_origin_crs in ('NBY', 'BDW') or planned_destination_crs in ('NBY', 'BDW'))
group by 1, 2 order by 3 desc
```

Delay minutes to GWR trains between Newbury and Southcote Junction (the section through Thatcham), by period and responsibility:

```sql
select financial_year_period, sum(delay_minutes) as delay_minutes,
       sum(delay_minutes) filter (where responsible_party = 'Network Rail') as network_rail,
       sum(delay_minutes) filter (where responsible_party = 'Operator') as operator
from lake.nr_delay_attribution
where toc_code = 'EF'
  and 'NEWBURY' in (event_start_name, event_end_name)
  and 'SOUTHCOTE JN' in (event_start_name, event_end_name)
group by 1 order by 1
```

## Source ETL / code

`rail_data_model/sql/nr_delay_attribution.sql` in the [rail-etl repo](https://github.com/DanteLore/rail-etl), rebuilt by the `model_rail_data` Lambda daily at 05:45 UTC. The corridor STANOX codes are listed in the SQL; they match the glossary's codes for the nine stations.

## Freshness & update cadence

Rebuilt daily. New data arrives when Network Rail publishes a period, about every four weeks with a lag of six to ten weeks.

**History from:** 2026-02-01 to 2026-08-22 on 2026-11-11 (157,000 rows over seven periods).

## Running costs (estimated)

Estimated monthly cost in USD at AWS eu-west-1 list prices, from the first build on 2026-11-11 (6.6 MB, 9 s, 98.5 MB scanned). Method: [rail-etl README](../README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| Athena (nightly rebuild) | 30 × 98.5 MB, growing about 14 MB a period | $0.015 now, about $0.04 after a year |
| S3 storage | 6.6 MB, about 1 MB a period | <$0.001 |
| Lambda `model_rail_data` share | 9 s × 128 MB × 30 | <$0.001 |
| **Total** |  | **$0.015 now, about $0.04 after a year** |

## Known issues & caveats

- **Only attributed incidents.** On-time trains aren't here, so pair it with RTT or HSP for rates.
- **Delays are recorded between timing points,** not at every station. Between Newbury and Reading most delays are on the Newbury–Southcote Junction section, so filter on that rather than `event_start_crs = 'THA'`.
- **The corridor filter is broad.** Any train starting or ending at Reading or Paddington is included, which covers most of the Great Western main line and the Elizabeth line. Filter on stations to narrow it.
- **A train can have several rows**: one per incident event, and a part-cancelled train can have both a delay and a cancellation row. Count distinct `trust_train_id` and `origin_departure_date` for trains.
- **Recent periods aren't final**: disputed attributions can change cause or responsibility in a later release.
- **Matching to RTT, HSP or the timetable** goes on `headcode`, the date and the planned origin time; TRUST IDs aren't schedule UIDs.

## Interested parties

The Thatcham rail page analysis.

## Status

Active.
