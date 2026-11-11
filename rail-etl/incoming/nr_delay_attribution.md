# incoming.nr_delay_attribution — Network Rail historic delay attribution

## Description

Every attributed delay and cancellation to GB passenger trains, as recorded in Network Rail's Performance System Strategy (PSS) database. One row per train per incident event: when a train is delayed by two incidents, or delayed and then cancelled, it has a row for each. Each row says where the delay happened, the incident that caused it, the cause code and who was held responsible. All of GB is loaded. Use [`lake.nr_delay_attribution`](../lake/nr_delay_attribution.md) for the corridor, decoded with the glossary.

The glossary that decodes the codes is loaded alongside it, as five small tables (no partitions, replaced when a new glossary arrives):

| Table | Rows | What it decodes |
|---|---|---|
| `incoming.nr_da_incident_reasons` | 366 | `incident_reason` → name, description, category, `cause_group` (Network Rail's JPIP category, e.g. Track, Non-Track Assets, Fleet, Traincrew, External) and `cause_super_group_code` |
| `incoming.nr_da_responsible_managers` | 6,994 | `responsible_manager` → manager name, organisation, and `responsible_party`: `Network Rail`, `TOC/FOC` (an operator) or `OTHERS` |
| `incoming.nr_da_reactionary_reasons` | 366 | `react_reason` → description and name |
| `incoming.nr_da_stanox` | 11,520 | STANOX location code → name, CRS code, route |
| `incoming.nr_da_operators` | 109 | `toc_code` → operator name, e.g. `EF` GWR, `EX` Elizabeth line |

## Classification

Public. Operational data with no personal information. Free-text incident descriptions occasionally mention things like trespass or disorder but don't name people.

## Owner

Dan (data platform).

## Source & references

Network Rail's "NWR Historic Delay Attribution" product on the [Rail Data Marketplace](https://raildata.org.uk/dashboard/dataProduct/P-b5f4216b-4de5-412f-9c9a-d14c604a6b9e/overview). One zipped CSV per railway period, named like `Transparency 26-27 P05 20261105.zip` (financial year, period, release date), plus the `Transparency page Attribution Glossary.xlsx` workbook. Field meanings: Network Rail's [Explanation document: Historic attributed delays](https://www.networkrail.co.uk/wp-content/uploads/2021/05/Explanation-of-historic-attributed-delays.pdf).

The marketplace's "file transfer" delivers files to `s3://dantelore-raildata-transfer/delay-attribution/`, using the access key of the IAM user `raildata-marketplace-transfer` (Terraform: `terraform/raildata_transfer.tf`; the key itself was created with the AWS CLI so its secret isn't in the Terraform state). Manually downloaded files can be dropped into the same folder.

## License & usage restrictions

Open Government Licence v3.

## Location

- S3: `s3://dantelore.data.incoming/nr/delay_attribution/financial_year_period=<YYYY-YY_Pnn>/nr_delay_attribution_<YYYY-YY_Pnn>.parquet`
- Glossary: `s3://dantelore.data.incoming/nr/delay_attribution_glossary/<table>/<table>.parquet`
- Glue: `incoming.nr_delay_attribution`, `incoming.nr_da_*`
- Format: Parquet, partitioned by `financial_year_period`

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| origin_departure_date | date | No | Key | The train's date (the day it left its origin) |
| trust_train_id | string | No | Key | TRUST train ID, e.g. `741K10MZ26`: 2-digit origin area, headcode (`1K10`), 2 more characters, then the day of the month. Unique within a period, not a year. Not the schedule UID |
| planned_origin_location_code | string | No | | Planned origin STANOX |
| planned_origin_wtt_datetime | timestamp | Yes | | Working timetable departure from origin |
| planned_origin_gbtt_datetime | timestamp | Yes | | Public timetable departure; null for trains not in the public timetable |
| planned_dest_location_code | string | No | | Planned destination STANOX |
| planned_dest_wtt_datetime | timestamp | Yes | | Working timetable arrival at destination |
| planned_dest_gbtt_datetime | timestamp | Yes | | Public timetable arrival |
| train_service_code | string | Yes | | Train service code where the delay happened |
| service_group_code | string | Yes | | Schedule 8 service group, e.g. `EF01` |
| toc_code | string | No | | Operator that ran the train, e.g. `EF` GWR, `EX` Elizabeth line |
| english_day_type | string | Yes | | `WD`, `SA`, `SU`, bank holiday or Christmas |
| applicable_timetable_flag | string | Yes | | `N` if the train isn't in official performance records (usually a short-term reinstatement of part of a cancelled train) |
| train_schedule_type | string | Yes | | E.g. `Permanent`, `STP Overlay`. Not validated by Network Rail |
| traction_type, trailing_load, timing_load, unit_class | string | Yes | | Not validated by Network Rail; often null |
| incident_number | string | No | Key | TRUST incident number. Unique only together with `incident_create_date` |
| incident_create_date | timestamp | Yes | Key | When the incident was entered |
| incident_start_datetime, incident_end_datetime | timestamp | Yes | | When the incident was live in the system (not how long it lasted on the ground) |
| section_code | string | Yes | | Where the incident happened |
| nr_location_manager | string | Yes | | Network Rail area |
| responsible_manager | string | Yes | | Who is responsible; decode with `nr_da_responsible_managers` |
| incident_reason | string | Yes | | Delay Attribution Guide cause code, e.g. `IB` points failure; decode with `nr_da_incident_reasons` |
| attribution_status | string | Yes | | `Attribution Agreed`, `Attribution Disputed` or `Waiting Acceptance` |
| incident_equipment | string | Yes | | Internal free text |
| incident_description | string | Yes | | Short internal description, e.g. `1J17 DISORDER RAM` |
| react_reason | string | Yes | | Reactionary reason code (`Y…`). Null means a primary delay, at the site of the incident |
| incident_resp_train | string | Yes | | Headcode of the train that caused the incident, if any |
| resp_train | string | Yes | | TRUST ID of the train responsible |
| react_train | string | Yes | | For a reactionary delay, the TRUST ID of the train that made this one late |
| event_type | string | No | | `A` automatic delay, `M` manual delay, `F` failed to stop, `C` cancelled at origin, `S` scheduled cancellation, `P` cancelled en route, `O` changed origin, `D` diverted |
| start_stanox, end_stanox | string | Yes | | Where the delay happened (not the incident) |
| event_datetime | timestamp | Yes | | When the train hit the delay |
| pfpi_minutes | double | Yes | | Delay minutes. For cancellations these are deemed minutes for internal use, not real delay: exclude them when summing delay |
| non_pfpi_minutes | double | Yes | | Delay minutes outside the performance regime |
| financial_year_period | string | No | Partition key | Railway period, e.g. `2026-27_P05` (April-to-March year, 13 four-week periods) |

## Source ETL / code

`nr_delay_attribution_etl/incoming_lambda/` in the [rail-etl repo](https://github.com/DanteLore/rail-etl), running as the `load_nr_delay_attribution_data` Lambda daily at 05:10 UTC. Each run loads any `.zip`, `.csv` or `.xlsx` in the transfer folder that isn't yet tagged `loaded=true`, then tags it. It handles a zip of zips (the marketplace's bulk download), converts dates and times, and keeps the latest release of each period, using the date in the file name and the partition file's `release_date` metadata. `delay_attribution.py` defines the schema and the glossary sheets. Tests are in `tests/test_delay_attribution.py`.

## Freshness & update cadence

Network Rail publishes a period about six to ten weeks after it ends, and re-releases periods as disputes are settled (period 2 of 2026-27 came out three times). The Lambda checks daily.

**History from:** 2026-02-01 (2025-26 period 12) to 2026-08-22 (2026-27 period 5), the ten files the marketplace offered on 2026-11-11. Network Rail says the dataset goes back to 2015, but older periods weren't offered.

## Running costs (estimated)

Estimated monthly cost in USD at AWS eu-west-1 list prices, from measurements taken 2026-11-11: seven periods (153 MB, 3.4 million rows), the glossary (0.4 MB) and the first Lambda run (160 s for all seven periods, 887 MB peak). Excludes tax and the AWS free tier (which currently covers all Lambda usage). Method: [rail-etl README](../README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | About 22 MB a period, 13 periods a year | $0.004 now, about $0.010 after a year |
| S3 storage, transfer bucket | Delivered zips (the first bulk download was 196 MB) | $0.005 |
| Lambda `load_nr_delay_attribution_data` | 30 short checks plus about one 25 s load a month, 1,024 MB | <$0.001 |
| S3 reads and writes | Daily listing and tag checks, about 2 files a month | <$0.001 |
| Athena (loader) | `ADD PARTITION` statements — DDL is free | $0 |
| **Total** |  | **About $0.01** |

Querying it: a period is about 22 MB; selecting a few columns scans much less.

## Known issues & caveats

- **Only attributed incidents.** Trains that ran on time aren't here, so this can't give a cancellation rate on its own. Use RTT or HSP for the count of trains run.
- **Cancellations carry deemed delay minutes.** Network Rail says to exclude cancellation events when summing `pfpi_minutes`.
- **Not final until agreed.** In the latest period (2026-27 P05), about 11% of corridor rows are `Attribution Disputed` and 2% `Waiting Acceptance`; their cause or responsibility can change in a later release.
- **Responsibility is remapped** to the operator map at the time of the extract.
- **Train IDs aren't schedule UIDs.** Match to RTT or the timetable on headcode, date and planned origin time.
- **The glossary lags new codes:** about 0.3% of corridor rows have a responsible manager the glossary doesn't list, and a few operator codes are missing.

## Interested parties

The Thatcham rail page analysis (the analyst's request for longer cancellation history and responsibility groups).

## Status

Active.
