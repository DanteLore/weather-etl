# Census 2011 Travel to Work (QS701EW and WU03EW)

## Description

Census 2011 method of travel to work, for comparison with Census 2021 and because many local plans' trip rates are still built on 2011 travel patterns. Two tables, all of England and Wales, on 2011 boundaries:

| Table | Contents | Levels |
|---|---|---|
| `incoming.nomis_census_2011_travel_method` | QS701EW: residents aged 16–74 by main method of travel to work, one row per area | Output area (181,408), LSOA, MSOA, 2011 ward, local authority |
| `incoming.nomis_census_2011_commute_flows` | WU03EW: workers by home area, workplace area and method of travel | MSOA (2.4 million non-zero flows), local authority (94,418) |

Use [ons_census_area_lookup_2011_2021](ons_census_area_lookup_2011_2021.md) to put 2011 areas on 2021 boundaries, and the 2021 equivalents [nomis_census_travel_method](nomis_census_travel_method.md) and [nomis_census_commute_flows](nomis_census_commute_flows.md).

## Classification

Public. Aggregated Census data; no personal data.

## Owner

Dan. See the gov-etl repo for the code.

## Source & references

ONS Census 2011 via Nomis: QS701EW (dataset NM_568_1) https://www.nomisweb.co.uk/census/2011/qs701ew and WU03EW (NM_1208_1) https://www.nomisweb.co.uk/census/2011/wu03ew. The MSOA flows come from the ONS release file `wu03ew_v2.csv` as archived by Jisc for the UK Data Service, because Nomis's whole-table download is every MSOA against every area (54 million rows, 96% zeros). MSOA names from the ONS MSOA (2011) to MSOA (2021) lookup.

## License & usage restrictions

Open Government Licence v3.0. Attribute ONS / Nomis.

## Location

- S3: `s3://dantelore.data.incoming/nomis_census_2011_travel_method/geography_level=<level>/…` and `s3://dantelore.data.incoming/nomis_census_2011_commute_flows/geography_level=<level>/…`
- Glue: `incoming.nomis_census_2011_travel_method`, `incoming.nomis_census_2011_commute_flows`
- Format: Parquet, partitioned by `geography_level`: `oa`, `lsoa`, `msoa`, `ward`, `ltla` (travel method); `msoa`, `ltla` (flows)

## Field spec

### nomis_census_2011_travel_method

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| geography_code | string | No | Primary | 2011 area code (`E00…` OA, `E01…` LSOA, `E02…` MSOA, `E05…` ward, `E06…`/`E07…` local authority, pre-2015 boundaries) |
| geography_name | string | No | | Area name |
| all_methods | int | No | | All usual residents aged 16 to 74, including those not in employment |
| work_from_home | int | No | | Work mainly at or from home |
| underground_metro_tram, train, bus_coach, taxi, motorcycle, car_driver, car_passenger, bicycle, on_foot, other | int | No | | Main method of travel to work (longest part by distance) |
| not_in_employment | int | No | | Residents aged 16–74 not in employment |
| geography_level | string | No | Partition key | `oa`, `lsoa`, `msoa`, `ward`, `ltla` |

### nomis_census_2011_commute_flows

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| home_code, home_name | string | No | Key | Area of usual residence (2011 MSOA, or census merged local authority) |
| workplace_code, workplace_name | string | No | Key | Workplace area, or `OD0000001` mainly work at or from home, `OD0000002` offshore installation, `OD0000003` no fixed place, `OD0000004` outside UK; MSOA flows also have `S92000003` Scotland and `N92000002` Northern Ireland |
| all_methods | int | No | | Workers in employment aged 16+ (16–74 in QS701EW) |
| work_from_home … other | int | No | | By method, as in the travel method table (WU03EW uses the 2001 categories, the same eleven) |
| geography_level | string | No | Partition key | `msoa` or `ltla` |

## Common queries

Cycling to work in Thatcham's MSOAs, 2011 against 2021 (the four MSOAs have the same codes in both):

```sql
select '2011' as census, geography_code, bicycle, all_methods - not_in_employment - work_from_home as commuters
from incoming.nomis_census_2011_travel_method
where geography_level = 'msoa' and geography_code in ('E02003377', 'E02003380', 'E02003382', 'E02003383')
union all
select '2021', geography_code, bicycle, total - work_from_home
from incoming.nomis_census_travel_method
where geography_level = 'msoa' and geography_code in ('E02003377', 'E02003380', 'E02003382', 'E02003383')
order by 2, 1
```

Where Thatcham residents worked in 2011, and how many cycled:

```sql
select workplace_name, sum(all_methods) as workers, sum(bicycle) as cyclists, sum(car_driver) as drivers
from incoming.nomis_census_2011_commute_flows
where geography_level = 'msoa' and home_code in ('E02003377', 'E02003380', 'E02003382', 'E02003383')
group by 1 order by 2 desc limit 15
```

## Source ETL / code

`github.com/dantelore/gov-etl`, `nomis_census_2011/nomis_census_2011_load.py`.

```bash
python nomis_census_2011/nomis_census_2011_load.py
python nomis_census_2011/nomis_census_2011_load.py --table nomis_census_2011_commute_flows
```

## Freshness & update cadence

Census 2011 snapshot, loaded 2026-11-12. Static.

## Running costs (estimated)

From the S3 size on 2026-11-12. Method: [gov-etl README](README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.017 GB, 7 objects | <$0.001 |
| **Total** | | **<$0.001** |

## Known issues & caveats

- **The two censuses count different people.** QS701EW counts residents aged 16–74 (with "not in employment" as a category); 2021's TS061 counts residents 16+ in employment. Compare shares of commuters (excluding home workers and those not working), not raw counts.
- **2021 was taken in lockdown**, so it understates commuting and overstates home working against 2011. A fall in any commuting mode between the two is partly the pandemic.
- **Boundaries changed.** Output areas and LSOAs were split and merged in 2021 (and new estates such as north-east Thatcham get new OAs); compare via [ons_census_area_lookup_2011_2021](ons_census_area_lookup_2011_2021.md). Thatcham's four MSOAs kept their codes.
- Output area flows (WF01BEW) aren't loaded: Nomis only offers them as every output area against every other (tens of billions of mostly zero rows) and there is no public file of the non-zero flows.
- Small counts are perturbed by ONS disclosure control.
- Local authorities are pre-2015 (2011) authorities; flows use census merged authorities (City of London with Westminster, Isles of Scilly with Cornwall).

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|
| Thatcham analysis (thatchamplan.co.uk) | Dan | What the council's 2011-based trip rates assumed; cycling growth 2011 to 2021 |

## Status

Active.
