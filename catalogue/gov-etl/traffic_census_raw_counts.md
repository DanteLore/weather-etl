# DfT Raw Traffic Counts (hourly manual counts)

## Description

The hour-by-hour manual traffic counts behind the DfT's annual average daily flow (AADF) figures: 5.3 million rows from 2000 to 2025. On a count day an enumerator counts every vehicle passing a count point from 07:00 to 19:00, by direction and vehicle type (cycles, motorcycles, cars, buses, vans, six classes of HGV). One row per count point, direction, date and hour. Gives peak-hour flows and the shape of the day on a road, which the yearly averages in [traffic_census](traffic_census.md) can't show.

## Classification

Public. Open government data; no personal data.

## Owner

Dan. See the gov-etl repo for the code.

## Source & references

Department for Transport road traffic statistics, file `dft_traffic_counts_raw_counts.zip` from https://roadtraffic.dft.gov.uk/downloads (about 1 GB of CSV). Metadata: https://roadtraffic.dft.gov.uk/about

## License & usage restrictions

Open Government Licence v3.0. Attribute DfT.

## Location

- S3: `s3://dantelore.data.incoming/traffic_census/raw_counts/year=<yyyy>/raw_counts_<yyyy>.parquet`
- Glue: `incoming.traffic_census_raw_counts`
- Format: Parquet, partitioned by `year` (string), like `traffic_census_aadf`

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| count_point_id | int | No | Key; joins to traffic_census_aadf | Count point |
| direction_of_travel | string | No | Key | `N`, `S`, `E`, `W` (both directions are counted separately) |
| count_date | date | No | Key | The day of the count |
| hour | int | No | Key | Hour the count started, `7` (07:00–08:00) to `18` (18:00–19:00) |
| region_id, region_name, region_ons_code | int, string, string | Yes | | Region |
| local_authority_id, local_authority_name, local_authority_code | int, string, string | Yes | | Local authority, e.g. `West Berkshire`, `E06000037` |
| road_name | string | Yes | | Road, e.g. `A4` |
| road_category | string | Yes | | DfT road category code, e.g. `PA` principal A road |
| road_type | string | Yes | | `Major` or `Minor` |
| start_junction_road_name, end_junction_road_name | string | Yes | | The junctions at each end of the counted link (major roads) |
| easting, northing | int | Yes | | OSGB36 location of the count point |
| latitude, longitude | double | Yes | | WGS84 location |
| link_length_km, link_length_miles | double | Yes | | Length of the link (major roads) |
| pedal_cycles | int | Yes | | Vehicles counted in the hour |
| two_wheeled_motor_vehicles | int | Yes | | |
| cars_and_taxis | int | Yes | | |
| buses_and_coaches | int | Yes | | |
| lgvs | int | Yes | | Light goods vehicles (vans) |
| hgvs_2_rigid_axle, hgvs_3_rigid_axle, hgvs_4_or_more_rigid_axle, hgvs_3_or_4_articulated_axle, hgvs_5_articulated_axle, hgvs_6_articulated_axle | int | Yes | | HGVs by axle class |
| all_hgvs | int | Yes | | All HGVs |
| all_motor_vehicles | int | Yes | | All motor vehicles (excludes cycles) |
| year | string | No | Partition key | Year of the count |

## Common queries

Count points with manual counts along the A4 and nearby roads between Newbury and Theale:

```sql
select count_point_id, road_name, start_junction_road_name, end_junction_road_name,
       min(count_date) as first_count, max(count_date) as last_count, count(distinct count_date) as count_days
from incoming.traffic_census_raw_counts
where easting between 444000 and 470000 and northing between 163000 and 173000
  and road_name in ('A4', 'A339', 'A34', 'A340')
group by 1, 2, 3, 4 order by 2, 1
```

Hourly profile on the A4 between Pipers Way, Thatcham and the A340 (count point 46128, both directions added together):

```sql
select count_date, hour, sum(all_motor_vehicles) as vehicles, sum(all_hgvs) as hgvs
from incoming.traffic_census_raw_counts
where count_point_id = 46128
group by 1, 2 order by 1, 2
```

The A4 through Thatcham has two count points: 78260 (A339 to Pipers Way, last counted 2020) and 46128 (Pipers Way to the A340, 13 count days up to March 2023).

## Source ETL / code

`github.com/dantelore/gov-etl`, `traffic_census/traffic_census_raw_counts_load.py`. Loads every year in one run.

## Freshness & update cadence

Annual, with the AADF release (the file loaded on 2026-11-12 was published June 2026 and runs to 2025). Rerun the loader after DfT's next release.

## Running costs (estimated)

From the S3 size on 2026-11-12. Method: [gov-etl README](README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.078 GB, 26 objects | $0.002 |
| **Total** | | **$0.002** |

Querying it: a year is about 3 MB.

## Known issues & caveats

- **One day per count, not every year.** Most count points are counted manually every few years (some only once); in the years between, the AADF figures are estimates. A single count day can be distorted by roadworks, weather or an incident nearby.
- Counts are 07:00 to 19:00 only, on weekdays in spring and autumn (DfT avoids school holidays). There is nothing for evenings, nights or weekends.
- Rows are per direction; add both directions for two-way flow.
- Some count points move or are replaced (e.g. 93156 replaced 80639 on the A339); link by road and junction names when comparing over time.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|
| Thatcham analysis (thatchamplan.co.uk) | Dan | Peak-hour flows on the A4 |

## Status

Active.
