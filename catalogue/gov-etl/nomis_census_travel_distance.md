# Census 2021 Distance Travelled to Work (TS058)

## Description

Census 2021 counts of how far working residents travel to work, in distance bands from under 2km to 60km and over, plus those working from home and those with no fixed workplace. One row per area, one column per band, for every output area (OA), LSOA, MSOA, local authority and ward in England and Wales. Distance is the straight line from home to workplace. Useful alongside travel method to see whether short trips that could be walked or cycled are being driven.

## Classification

Public. Aggregated Census data; no personal data.

## Owner

Dan. See the gov-etl repo for the code.

## Source & references

ONS Census 2021, topic summary TS058 "Distance travelled to work", via Nomis (dataset NM_2075_1): bulk download `census2021-ts058.zip` for OAs to local authorities, Nomis API for 2022 wards. https://www.nomisweb.co.uk/sources/census_2021_bulk

## License & usage restrictions

Open Government Licence v3.0. Attribute ONS / Nomis.

## Location

- S3: `s3://dantelore.data.incoming/nomis_census_travel_distance/geography_level=<level>/nomis_census_travel_distance_<level>.parquet`
- Glue: `incoming.nomis_census_travel_distance`
- Format: Parquet, partitioned by `geography_level`: `oa`, `lsoa`, `msoa`, `ltla`, `ward`

Always filter on `geography_level`.

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| geography_code | string | No | Primary (with geography_level) | ONS area code (see [nomis_census_travel_method](nomis_census_travel_method.md)) |
| geography_name | string | No | | Area name |
| total | int | No | | Usual residents aged 16+ in employment the week before the census |
| under_2km | int | No | | Less than 2km |
| from_2_to_5km | int | No | | 2km to less than 5km |
| from_5_to_10km | int | No | | 5km to less than 10km |
| from_10_to_20km | int | No | | 10km to less than 20km |
| from_20_to_30km | int | No | | 20km to less than 30km |
| from_30_to_40km | int | No | | 30km to less than 40km |
| from_40_to_60km | int | No | | 40km to less than 60km |
| km_60_and_over | int | No | | 60km and over |
| work_from_home | int | No | | Works mainly from home |
| offshore_no_fixed_place_or_abroad | int | No | | Works mainly at an offshore installation, in no fixed place, or outside the UK |
| geography_level | string | No | Partition key | `oa`, `lsoa`, `msoa`, `ltla` or `ward` |

## Common queries

Share of Thatcham commuters travelling under 5km, by ward (excluding home workers):

```sql
select geography_name,
       round(100.0 * (under_2km + from_2_to_5km) / (total - work_from_home), 1) as pct_under_5km,
       round(100.0 * (from_20_to_30km + from_30_to_40km + from_40_to_60km + km_60_and_over) / (total - work_from_home), 1) as pct_20km_plus
from incoming.nomis_census_travel_distance
where geography_level = 'ward' and geography_name like 'Thatcham%'
```

## Source ETL / code

`github.com/dantelore/gov-etl`, `nomis_census_travel/nomis_census_travel_load.py --table nomis_census_travel_distance`.

## Freshness & update cadence

Census 2021 snapshot, loaded 2026-11-12. Static.

## Running costs (estimated)

From the S3 size on 2026-11-12. Method: [gov-etl README](README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.006 GB, 5 objects | <$0.001 |
| **Total** | | **<$0.001** |

## Known issues & caveats

- **Census 2021 was taken during lockdown**, so `work_from_home` is overstated and the distance bands hold fewer people than a normal year. See the caveat in [nomis_census_travel_method](nomis_census_travel_method.md).
- Distances are straight lines between home and workplace postcodes, not road distances.
- Counts are perturbed slightly by ONS disclosure control, most noticeably at OA level.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|
| Thatcham analysis (thatchamplan.co.uk) | Dan | |

## Status

Active.
