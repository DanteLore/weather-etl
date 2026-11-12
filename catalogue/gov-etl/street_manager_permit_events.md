# Street Manager Roadworks Permits, England

## Description

Every roadworks permit event in England from the DfT's Street Manager (the "manage roadworks" service): permits submitted, granted, refused, altered, cancelled and revoked, and works starting and stopping. One row per event, by utilities (water, gas, electricity, telecoms, EV chargers) and by highway authorities (councils), with the street, location (as an OSGB line or point), dates, traffic management (e.g. road closure, lights) and status. Every highway authority in England (about 160) that uses Street Manager, July 2020 onwards; about a million events a month. For explaining disruption to buses and traffic. Filter on `highway_authority_swa_code = '340'` for West Berkshire.

## Classification

Public. Open government data; no personal data (promoters are organisations).

## Owner

Dan. See the gov-etl repo for the code.

## Source & references

Department for Transport, Street Manager open data: monthly archives at `https://opendata.manage-roadworks.service.gov.uk/permit/YYYY/MM.zip` (about 1 GB and a million JSON event files each, all of England). Data dictionary: https://department-for-transport-streetmanager.github.io/street-manager-docs/ and https://www.gov.uk/guidance/find-and-use-roadworks-data

## License & usage restrictions

Open Government Licence v3.0. Attribute DfT.

## Location

- S3: `s3://dantelore.data.incoming/street_manager/permit_events/event_month=<YYYY-MM>/street_manager_permit_events_<YYYY-MM>.parquet`
- Glue: `incoming.street_manager_permit_events`
- Format: Parquet, partitioned by `event_month` (the archive month)

## Field spec

All the permit fields are strings, as Street Manager sends them; `object_data_json` keeps the whole record.

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| event_reference | bigint | No | Primary | Street Manager event ID |
| event_type | string | No | | `PERMIT_SUBMITTED`, `PERMIT_GRANTED`, `PERMIT_REFUSED`, `PERMIT_ALTERATION_GRANTED`, `PERMIT_CANCELLED`, `PERMIT_REVOKED`, `WORK_START`, `WORK_STOP`, `WORK_START_REVERTED`, `CURRENT_TRAFFIC_MANAGEMENT_UPDATED`, … |
| event_time | timestamp | No | | When the event happened, UTC |
| object_type | string | No | | Always `PERMIT` in this archive |
| object_reference | string | No | | The permit reference |
| version | int | Yes | | Event schema version |
| work_reference_number | string | Yes | Key | The works; one works can have several permits |
| permit_reference_number | string | Yes | | E.g. `5A002-CKBER-P-014275-01` |
| promoter_swa_code, promoter_organisation | string | Yes | | Who is doing the work, e.g. `Thames Water`, `Connected Kerb`, `WEST BERKSHIRE COUNCIL` |
| highway_authority, highway_authority_swa_code | string | Yes | | Highway authority and its street works code, e.g. `WEST BERKSHIRE COUNCIL`, `340`; `READING BOROUGH COUNCIL`, `345`. Names are written inconsistently (`ESSEX COUNTY COUNCIL`, `West Northamptonshire Council`), so filter on the code |
| works_location_coordinates | string | Yes | | WKT geometry in OSGB36 (EPSG:27700), e.g. `LINESTRING(467850.4 171503.6, …)` or `POINT(…)` |
| street_name, area_name, town | string | Yes | | Where |
| usrn | string | Yes | | Unique Street Reference Number |
| road_category | string | Yes | | Street reinstatement category, `0` (busiest) to `4` |
| is_traffic_sensitive | string | Yes | | `Yes`/`No` |
| work_category | string | Yes | | `Minor`, `Standard`, `Major`, `Major (PAA)` (provisional advance authorisation), `Immediate - urgent`, `Immediate - emergency` |
| activity_type | string | Yes | | E.g. `Utility asset works`, `Highway repair and maintenance works` |
| traffic_management_type | string | Yes | | `Some carriageway incursion`, `No carriageway incursion`, `Road closure`, `Two-way signals`, `Multi-way signals`, `Give and take`, `Lane closure`, `Priority working`, `Stop/go boards`, `Contra-flow`, `Convoy workings` |
| works_location_type | string | Yes | | Carriageway, footway, verge |
| is_ttro_required | string | Yes | | Whether a temporary traffic regulation order is needed |
| work_status, permit_status | string | Yes | | E.g. `Works planned`, `Works in progress`, `Works completed`; `submitted`, `granted`, `closed` |
| proposed_start_date, proposed_start_time, proposed_end_date, proposed_end_time | string | Yes | | Planned dates (ISO 8601, UTC) |
| actual_start_date_time, actual_end_date_time | string | Yes | | Actual dates (ISO 8601, UTC) |
| permit_conditions | string | Yes | | National condition codes, e.g. `NCT01a, NCT11b` |
| close_footway | string | Yes | | Footway closure |
| collaborative_working | string | Yes | | Shared works |
| is_deemed | string | Yes | | Permit granted by default because the authority didn't respond in time |
| object_data_json | string | No | | The full `object_data` record as JSON |
| event_month | string | No | Partition key | `YYYY-MM` of the archive |

## Common queries

Road closures and temporary signals on the A4 (or any street) by month:

```sql
select event_month, street_name, traffic_management_type, count(distinct work_reference_number) as works
from incoming.street_manager_permit_events
where highway_authority_swa_code = '340' and event_type = 'WORK_START'
  and traffic_management_type in ('Road closure', 'Multi-way signals', 'Two-way signals')
  and (street_name like '%BATH ROAD%' or street_name like 'A4%')
group by 1, 2, 3 order by 1, 4 desc
```

Works that started on a given day in Thatcham, to explain a bad day for the buses:

```sql
select street_name, promoter_organisation, traffic_management_type, actual_start_date_time, proposed_end_date
from incoming.street_manager_permit_events
where event_month = '2026-09' and highway_authority_swa_code = '340' and event_type = 'WORK_START' and town = 'THATCHAM'
  and actual_start_date_time like '2026-09-15%'
```

## Source ETL / code

`github.com/dantelore/gov-etl`, `street_manager/street_manager_load.py`. Downloads each month's archive and writes every event to one parquet file a month (zstd, written in chunks of 100,000 events to keep memory down to about 2 GB). Many archives from 2020 to 2025 are streamed zips with no central directory (they won't open in `unzip`), so the loader reads them entry by entry from the local headers.

```bash
python street_manager/street_manager_load.py                  # every month not yet loaded
python street_manager/street_manager_load.py --month 2026-11  # (re)load one month
python street_manager/street_manager_load.py --reload-all     # reload every month
```

## Freshness & update cadence

Manual. A month's archive is complete after the month ends. Rerun the loader monthly (it loads any month not yet in S3).

**History from:** July 2020 (Street Manager started that month, so it is part-month) to October 2026. The West Berkshire-only load of 2026-11-12 had 220,985 events for 42,516 works; nationally a month has about a million events.

## Running costs (estimated)

From the S3 size on 2026-11-12. Method: [gov-etl README](README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | Estimated 7–8 GB for 76 months (about 100 MB a month, zstd), growing about 1.2 GB a year | About $0.17, rising about $0.03 a year |
| **Total** | | **About $0.17** |

The national reload started on 2026-11-12 and these are estimates from its first months; until it finishes, months it hasn't reached yet still hold West Berkshire only.

## Known issues & caveats

- **Events, not works.** A works shows up as several events (submitted, granted, started, stopped, and any alterations). Count distinct `work_reference_number`s, and take the latest event per permit for its final state.
- Field names and values changed over time; early events (2020) lack some fields. The whole record is in `object_data_json`.
- Local authority roads only. Works on motorways and trunk roads (National Highways, e.g. the M4 and A34) aren't in Street Manager, and some authorities joined after July 2020.
- A full scan reads several GB; select only the columns you need (`object_data_json` is most of the size).
- Dates are ISO strings in UTC; `proposed_*` dates for whole-day works are stored as 23:00 the evening before in summer.
- Street names are uppercase as entered (`BATH ROAD`, sometimes `A4 BATH ROAD`); match loosely.
- Events whose archive entry was cut off at the end of a damaged archive are lost (the loader stops at a truncated final entry).

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|
| Thatcham analysis (thatchamplan.co.uk) | Dan | Explaining bus and traffic disruption |

## Status

Active.
