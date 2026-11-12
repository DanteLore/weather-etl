# Census 2021 Car or Van Availability (TS045)

## Description

Census 2021 counts of households by the number of cars or vans they have: none, one, two, or three or more. One row per area for every output area (OA), LSOA, MSOA, local authority and ward in England and Wales. The standard measure of car ownership and car dependency; households with no car are the ones that rely on buses, walking and lifts.

## Classification

Public. Aggregated Census data; no personal data.

## Owner

Dan. See the gov-etl repo for the code.

## Source & references

ONS Census 2021, topic summary TS045 "Car or van availability", via Nomis (dataset NM_2063_1): bulk download `census2021-ts045.zip` for OAs to local authorities, Nomis API for 2022 wards. https://www.nomisweb.co.uk/sources/census_2021_bulk

## License & usage restrictions

Open Government Licence v3.0. Attribute ONS / Nomis.

## Location

- S3: `s3://dantelore.data.incoming/nomis_census_car_availability/geography_level=<level>/nomis_census_car_availability_<level>.parquet`
- Glue: `incoming.nomis_census_car_availability`
- Format: Parquet, partitioned by `geography_level`: `oa`, `lsoa`, `msoa`, `ltla`, `ward`

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| geography_code | string | No | Primary (with geography_level) | ONS area code (see [nomis_census_travel_method](nomis_census_travel_method.md)) |
| geography_name | string | No | | Area name |
| total_households | int | No | | All households |
| no_car | int | No | | No cars or vans in household |
| one_car | int | No | | 1 car or van |
| two_cars | int | No | | 2 cars or vans |
| three_or_more_cars | int | No | | 3 or more cars or vans |
| geography_level | string | No | Partition key | `oa`, `lsoa`, `msoa`, `ltla` or `ward` |

## Common queries

Households without a car, and cars per household (counting "3 or more" as 3), Thatcham wards against West Berkshire:

```sql
select geography_level, geography_name, total_households,
       round(100.0 * no_car / total_households, 1) as pct_no_car,
       round((one_car + 2.0 * two_cars + 3.0 * three_or_more_cars) / total_households, 2) as cars_per_household
from incoming.nomis_census_car_availability
where (geography_level = 'ward' and geography_name like 'Thatcham%')
   or (geography_level = 'ltla' and geography_name = 'West Berkshire')
```

In Thatcham Central 14% of households have no car (449 of 3,137); across West Berkshire it is 12%.

## Source ETL / code

`github.com/dantelore/gov-etl`, `nomis_census_travel/nomis_census_travel_load.py --table nomis_census_car_availability`.

## Freshness & update cadence

Census 2021 snapshot, loaded 2026-11-12. Static.

## Running costs (estimated)

From the S3 size on 2026-11-12. Method: [gov-etl README](README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.004 GB, 5 objects | <$0.001 |
| **Total** | | **<$0.001** |

## Known issues & caveats

- Car availability was less affected by lockdown than travel to work, but the census was still taken in March 2021.
- "3 or more" is open-ended, so a count of cars is a lower bound. Nomis dataset TS045A (not loaded) gives the number of cars per area directly.
- Counts include vans available for private use, and company cars, but not vans used only for work.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|
| Thatcham analysis (thatchamplan.co.uk) | Dan | |

## Status

Active.
