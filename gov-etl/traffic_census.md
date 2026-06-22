# DfT AADF Road Traffic Census

## Description

Annual Average Daily Flow (AADF) counts from the Department for Transport's road traffic
census. ~578,000 count-point-year records from 2000 to the present, covering major and
minor roads across Great Britain. Each record gives the average number of vehicles per day
passing a fixed count point in a given year, broken down by vehicle type (cars, HGVs,
cycles, etc.). Useful for understanding traffic volumes, road usage trends, and transport
infrastructure analysis.

## Classification

Public. Open government data; no personal data.

## Owner

Dan. See gov-etl repo for the code.

## Source & references

Department for Transport — Road Traffic Statistics.
https://roadtraffic.dft.gov.uk/downloads

## License & usage restrictions

Open Government Licence v3.0. Free to use with attribution to DfT.

## Location

- S3: `s3://dantelore.data.incoming/traffic_census/aadf/year=<yyyy>/`
- Glue: `incoming.traffic_census_aadf`
- Format: Parquet, partitioned by `year` (string, e.g. `2023`)

Always filter on `year` to limit the scan to the years of interest.

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| count_point_id | int | No | Primary (with year) | Unique ID for the traffic count point |
| direction_of_travel | string | Yes | | Direction of flow (e.g. `N`, `S`, `E`, `W`, `Combined`) |
| region_id | int | Yes | | DfT region identifier |
| region_name | string | Yes | | Region name (e.g. `South East`) |
| local_authority_id | int | Yes | | Local authority identifier |
| local_authority_name | string | Yes | | Local authority name |
| road_name | string | Yes | | Road identifier (e.g. `A34`) |
| road_category | string | Yes | | Road category code |
| road_type | string | Yes | | `Major` or `Minor` |
| start_junction_road_name | string | Yes | | Road name at start junction |
| end_junction_road_name | string | Yes | | Road name at end junction |
| easting | int | Yes | | OSGB36 easting of count point (metres) — note: column named `easting` not `x_coordinate` |
| northing | int | Yes | | OSGB36 northing of count point (metres) — note: column named `northing` not `y_coordinate` |
| latitude | double | Yes | | WGS84 latitude |
| longitude | double | Yes | | WGS84 longitude |
| link_length_km | double | Yes | | Length of the counted road link (km) |
| link_length_miles | double | Yes | | Length of the counted road link (miles) |
| pedal_cycles | int | Yes | | AADF — pedal cycles |
| two_wheeled_motor_vehicles | int | Yes | | AADF — motorcycles and mopeds |
| cars_and_taxis | int | Yes | | AADF — cars and taxis |
| buses_and_coaches | int | Yes | | AADF — buses and coaches |
| lgvs | int | Yes | | AADF — light goods vehicles |
| hgvs_2_rigid_axle | int | Yes | | AADF — HGVs, 2-axle rigid |
| hgvs_3_rigid_axle | int | Yes | | AADF — HGVs, 3-axle rigid |
| hgvs_4_or_more_rigid_axle | int | Yes | | AADF — HGVs, 4+ axle rigid |
| hgvs_3_or_4_articulated_axle | int | Yes | | AADF — HGVs, 3–4 axle articulated |
| hgvs_5_articulated_axle | int | Yes | | AADF — HGVs, 5-axle articulated |
| hgvs_6_articulated_axle | int | Yes | | AADF — HGVs, 6-axle articulated |
| all_hgvs | int | Yes | | AADF — all HGVs combined |
| all_motor_vehicles | int | Yes | | AADF — all motor vehicles combined |
| year | string | No | Partition key | Census year (e.g. `2023`) |

## Source ETL / code

`github.com/dantelore/gov-etl`, `traffic_census/traffic_census_bulk_load.py`.
Supports loading all years or a specific year via `--year` flag.

## Freshness & update cadence

DfT publish updated counts annually, typically in the autumn for the previous year.
Re-run the loader for the new year when published:
```bash
python traffic_census/traffic_census_bulk_load.py --year 2024
```

## Known issues & caveats

- Coordinate columns are named `easting`/`northing` (not `x_coordinate`/`y_coordinate`
  as in UPRN and elevation tables) — watch for this when joining spatially.
- AADF is a modelled average, not a direct count. Some count points have imputed values
  where the physical counter malfunctioned.
- `direction_of_travel` is `Combined` for many minor road count points where direction
  is not separately recorded.
- Count point locations are fixed but the road network around them changes — a count
  point on a road that was reclassified will show a discontinuity in some fields.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
