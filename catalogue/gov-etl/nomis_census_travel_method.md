# Census 2021 Method of Travel to Work (TS061)

## Description

Census 2021 counts of how working residents travel to work: work from home, train, bus, driving a car, car passenger, bicycle, on foot and so on. One row per area, one column per method. Covers every output area (OA), LSOA, MSOA, local authority and ward in England and Wales, so it can be read at street-block level or compared across towns. Resident-based: counts where people live, not where they work. Useful for car dependency, the share commuting by bus or train, and modal split around a development site.

## Classification

Public. Aggregated Census data; no personal data.

## Owner

Dan. See the gov-etl repo for the code.

## Source & references

ONS Census 2021, topic summary TS061 "Method used to travel to work", via Nomis (dataset NM_2078_1). Output areas to local authorities come from the Nomis bulk download (`census2021-ts061.zip`); wards (2022 boundaries) from the Nomis API. https://www.nomisweb.co.uk/sources/census_2021_bulk

## License & usage restrictions

Open Government Licence v3.0. Attribute ONS / Nomis.

## Location

- S3: `s3://dantelore.data.incoming/nomis_census_travel_method/geography_level=<level>/nomis_census_travel_method_<level>.parquet`
- Glue: `incoming.nomis_census_travel_method`
- Format: Parquet, partitioned by `geography_level`: `oa` (188,880 areas), `lsoa` (35,672), `msoa` (7,264), `ltla` (331 local authorities), `ward` (7,638)

Always filter on `geography_level`, or every person is counted five times.

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| geography_code | string | No | Primary (with geography_level) | ONS code: `E00…` OA, `E01…` LSOA, `E02…` MSOA, `E06…`/`E07…`/`E08…`/`E09…` local authority, `E05…` ward (Welsh codes start `W`). Joins to `nomis_census_occupation.ward_cd`, `ons_msoa_boundaries_msoa.msoa21cd`, `ons_postcode_lookup_lookup.msoa21cd` |
| geography_name | string | No | | Area name, e.g. `West Berkshire 014` (MSOA), `Thatcham Central` (ward); OAs are named by their code |
| total | int | No | | Usual residents aged 16+ in employment the week before the census |
| work_from_home | int | No | | Work mainly at or from home |
| underground_metro_tram | int | No | | Underground, metro, light rail, tram |
| train | int | No | | Train |
| bus_coach | int | No | | Bus, minibus or coach |
| taxi | int | No | | Taxi |
| motorcycle | int | No | | Motorcycle, scooter or moped |
| car_driver | int | No | | Driving a car or van |
| car_passenger | int | No | | Passenger in a car or van |
| bicycle | int | No | | Bicycle |
| on_foot | int | No | | On foot |
| other | int | No | | Other method |
| geography_level | string | No | Partition key | `oa`, `lsoa`, `msoa`, `ltla` or `ward` |

The categories add up to `total`.

## Common queries

How Thatcham's working residents get to work, by ward:

```sql
select geography_name, total,
       round(100.0 * work_from_home / total, 1) as pct_home,
       round(100.0 * (car_driver + car_passenger) / total, 1) as pct_car,
       round(100.0 * bus_coach / total, 1) as pct_bus,
       round(100.0 * train / total, 1) as pct_train
from incoming.nomis_census_travel_method
where geography_level = 'ward' and geography_name like 'Thatcham%'
```

For Thatcham Central this gives 34% working from home, 53% by car (48% driving), 1.4% by bus and 1.1% by train.

## Source ETL / code

`github.com/dantelore/gov-etl`, `nomis_census_travel/nomis_census_travel_load.py` (loads this table, `nomis_census_travel_distance` and `nomis_census_car_availability`). Shared code in `helpers/nomis.py` (`census_topic_levels`).

```bash
python nomis_census_travel/nomis_census_travel_load.py --table nomis_census_travel_method
```

## Freshness & update cadence

Census 2021 snapshot, loaded 2026-11-12. Static; the next census is expected in 2031.

## Running costs (estimated)

Estimated monthly cost in USD at AWS eu-west-1 list prices, from the S3 size on 2026-11-12. Method: [gov-etl README](README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.005 GB, 5 objects | <$0.001 |
| **Total** | | **<$0.001** |

Querying it: the whole table is 5 MB, under Athena's 10 MB minimum.

## Known issues & caveats

- **Census 2021 was taken during lockdown** (21 March 2021), when many people were told to work from home and public transport use was far below normal. Home working is overstated and train and bus commuting understated compared with today. ONS advises care using its travel-to-work data for planning. Compare with the 2011 Census or current DfT statistics before drawing conclusions about car or bus use.
- People on furlough were asked to answer for their usual job, which adds to the distortion.
- ONS applies targeted record swapping and small cell adjustment, so OA-level counts can be off by a few people.
- Wards are 2022 boundaries; the other levels use 2021 boundaries.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|
| Thatcham analysis (thatchamplan.co.uk) | Dan | Car dependency around the NE Thatcham site |

## Status

Active.
