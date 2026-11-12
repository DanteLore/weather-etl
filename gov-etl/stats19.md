# STATS19 Road Safety Data (collisions, vehicles, casualties)

## Description

Every personal-injury road collision reported to the police in Great Britain since 1979: 9.1 million collisions, 16.3 million vehicles and 12.1 million casualties. This is the DfT "road safety data", also called STATS19 after the police reporting form, or road accident / road casualty statistics. Each collision has a location (OSGB grid reference, and latitude/longitude from 1999), date and time, severity, road, speed limit, junction, light, weather and surface conditions. The vehicles and casualties tables give vehicle type and manoeuvre, driver age and sex, casualty class (driver, passenger, pedestrian) and severity. Damage-only collisions aren't included.

Three tables, all partitioned by `collision_year`:

| Table | Rows | One row per |
|---|---|---|
| `incoming.stats19_collisions` | 9,116,625 | collision |
| `incoming.stats19_vehicles` | 16,283,094 | vehicle in a collision |
| `incoming.stats19_casualties` | 12,102,133 | casualty |

## Classification

Public. Published open data. DfT removes details that could identify people, but collision locations and dates are exact, so take care when presenting individual serious or fatal collisions.

## Owner

Dan. See the gov-etl repo for the code.

## Source & references

Department for Transport, Road Safety Data: https://www.data.gov.uk/dataset/cb7ae6f0-4be6-4935-9277-47e5ce24a11f/road-safety-data. Files: `dft-road-casualty-statistics-{collision,vehicle,casualty}-1979-latest-published-year.csv` at data.dft.gov.uk/road-accidents-safety-data/. The codes in almost every column are decoded by the DfT's "Road Safety Open Dataset Data Guide" (an Excel file on the same page); it isn't loaded here.

## License & usage restrictions

Open Government Licence v3.0. Attribute DfT.

## Location

- S3: `s3://dantelore.data.incoming/stats19/<table>/collision_year=<yyyy>/<table>_<yyyy>.parquet`
- Glue: `incoming.stats19_collisions`, `incoming.stats19_vehicles`, `incoming.stats19_casualties`
- Format: Parquet, partitioned by `collision_year` (string, `1979` to `2025`)

Always filter on `collision_year`: the collisions table is 610 MB in all.

## Field spec

Column names and codes are as DfT publishes them. Codes are integers; `-1` means data missing or out of range and `99` usually unknown. Look codes up in the DfT data guide. `_historic` columns hold the old coding of a field whose codes changed (recent years use the newer column).

### stats19_collisions

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| collision_index | string | No | Primary; joins vehicles and casualties | Unique collision ID: year, police force and reference |
| collision_ref_no | string | No | | Police reference (unique within year and force) |
| location_easting_osgr | int | Yes | | OSGB36 easting (metres) |
| location_northing_osgr | int | Yes | | OSGB36 northing (metres) |
| longitude | double | Yes | | WGS84 longitude (blank before 1999) |
| latitude | double | Yes | | WGS84 latitude (blank before 1999) |
| police_force | int | No | | Police force code (Thames Valley is 43) |
| collision_severity | int | No | | `1` fatal, `2` serious, `3` slight |
| number_of_vehicles | int | No | | Vehicles involved |
| number_of_casualties | int | No | | Casualties |
| date | date | No | | Date of the collision |
| day_of_week | int | No | | `1` Sunday to `7` Saturday |
| time | string | Yes | | Time, `HH:MM` |
| local_authority_district | int | Yes | | Old DfT district code (`-1` in recent years) |
| local_authority_ons_district | string | Yes | | ONS district code, e.g. `E06000037` West Berkshire |
| local_authority_highway | string | Yes | | Highway authority code at the time |
| local_authority_highway_current | string | Yes | | Highway authority code on current boundaries |
| first_road_class | int | Yes | | `1` motorway, `2` A(M), `3` A, `4` B, `5` C, `6` unclassified |
| first_road_number | int | Yes | | Road number, e.g. `4` for the A4 (`0` unclassified) |
| road_type | int | Yes | | Roundabout, one way, dual or single carriageway, slip road |
| speed_limit | int | Yes | | Speed limit (mph) |
| junction_detail_historic, junction_detail | int | Yes | | Type of junction, if any |
| junction_control | int | Yes | | Traffic signals, give way, etc. |
| second_road_class, second_road_number | int | Yes | | The other road at a junction |
| pedestrian_crossing_human_control_historic, pedestrian_crossing_physical_facilities_historic, pedestrian_crossing | int | Yes | | Crossing facilities within 50m |
| light_conditions | int | Yes | | Daylight, darkness with or without lighting |
| weather_conditions | int | Yes | | Fine, rain, snow, fog, high winds |
| road_surface_conditions | int | Yes | | Dry, wet, ice, flood |
| special_conditions_at_site | int | Yes | | Signals out, roadworks, defective road, etc. |
| carriageway_hazards_historic, carriageway_hazards | int | Yes | | Objects or animals in the road |
| urban_or_rural_area | int | Yes | | `1` urban, `2` rural |
| did_police_officer_attend_scene_of_accident | int | Yes | | `1` yes, `2` no, `3` reported by self-completion |
| trunk_road_flag | int | Yes | | `1` trunk (National Highways), `2` non-trunk |
| lsoa_of_accident_location | string | Yes | | LSOA code (England and Wales, from 2004) |
| enhanced_severity_collision | int | Yes | | Severity under the injury-based reporting systems (CRASH/COPA) |
| collision_injury_based | int | Yes | | `1` if reported through an injury-based system |
| collision_adjusted_severity_serious | double | Yes | | Probability the collision would be serious under injury-based reporting (DfT adjustment) |
| collision_adjusted_severity_slight | double | Yes | | Probability it would be slight |
| collision_year | string | No | Partition key | Year of the collision |

### stats19_vehicles

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| collision_index | string | No | Key | Joins to the collision |
| collision_ref_no | string | No | | Police reference |
| vehicle_reference | int | No | Key (with collision_index) | Vehicle number within the collision |
| vehicle_type | int | Yes | | Car, motorcycle, pedal cycle, bus, goods vehicle, e-scooter, etc. |
| towing_and_articulation | int | Yes | | Towing or articulated |
| vehicle_manoeuvre_historic, vehicle_manoeuvre | int | Yes | | Turning, overtaking, parked, going ahead, etc. |
| vehicle_direction_from, vehicle_direction_to | int | Yes | | Compass direction of travel |
| vehicle_location_restricted_lane_historic, vehicle_location_restricted_lane | int | Yes | | Bus lane, cycle lane, footway, etc. |
| junction_location | int | Yes | | Where in the junction the vehicle was |
| skidding_and_overturning | int | Yes | | Skidded, jack-knifed, overturned |
| hit_object_in_carriageway, hit_object_off_carriageway | int | Yes | | What the vehicle hit |
| vehicle_leaving_carriageway | int | Yes | | Left the carriageway, and which side |
| first_point_of_impact | int | Yes | | Front, back, offside, nearside |
| vehicle_left_hand_drive | int | Yes | | Left-hand drive |
| journey_purpose_of_driver_historic, journey_purpose_of_driver | int | Yes | | Commuting, work, school run, other |
| sex_of_driver | int | Yes | | `1` male, `2` female, `3` not known |
| age_of_driver | int | Yes | | Age in years |
| age_band_of_driver | int | Yes | | Age band |
| engine_capacity_cc | int | Yes | | Engine size |
| propulsion_code | int | Yes | | Petrol, diesel, electric, hybrid, etc. |
| age_of_vehicle | int | Yes | | Years |
| generic_make_model | string | Yes | | Make and model, e.g. `MERCEDES SPRINTER` (recent years) |
| driver_imd_decile | int | Yes | | Deprivation decile of the driver's home area |
| lsoa_of_driver | string | Yes | | LSOA of the driver's home (England and Wales) |
| escooter_flag | int | Yes | | `1` if the vehicle was an e-scooter |
| driver_distance_banding | int | Yes | | Distance from the driver's home to the collision |
| collision_year | string | No | Partition key | Year of the collision |

### stats19_casualties

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| collision_index | string | No | Key | Joins to the collision |
| collision_ref_no | string | No | | Police reference |
| vehicle_reference | int | No | | The vehicle the casualty was in or hit by |
| casualty_reference | int | No | Key (with collision_index) | Casualty number within the collision |
| casualty_class | int | Yes | | `1` driver or rider, `2` passenger, `3` pedestrian |
| sex_of_casualty | int | Yes | | `1` male, `2` female |
| age_of_casualty | int | Yes | | Age in years |
| age_band_of_casualty | int | Yes | | Age band |
| casualty_severity | int | Yes | | `1` fatal, `2` serious, `3` slight |
| pedestrian_location | int | Yes | | Where a pedestrian was (on a crossing, footway, etc.) |
| pedestrian_movement | int | Yes | | What a pedestrian was doing |
| car_passenger | int | Yes | | Front or rear seat |
| bus_or_coach_passenger | int | Yes | | Boarding, alighting, standing, seated |
| pedestrian_road_maintenance_worker | int | Yes | | Road worker |
| casualty_type | int | Yes | | Pedestrian, cyclist, car occupant, bus occupant, etc. |
| casualty_imd_decile | int | Yes | | Deprivation decile of the casualty's home area |
| lsoa_of_casualty | string | Yes | | LSOA of the casualty's home |
| enhanced_casualty_severity | int | Yes | | Severity under injury-based reporting |
| casualty_injury_based | int | Yes | | `1` if reported through an injury-based system |
| casualty_adjusted_severity_serious | double | Yes | | Probability the casualty would be serious under injury-based reporting |
| casualty_adjusted_severity_slight | double | Yes | | Probability it would be slight |
| casualty_distance_banding | int | Yes | | Distance from the casualty's home to the collision |
| escooter_flag | int | Yes | | E-scooter involved |
| collision_year | string | No | Partition key | Year of the collision |

## Common queries

Injury collisions around Thatcham (easting 448000–456000, northing 165000–169500) by year:

```sql
select collision_year, count(*) as collisions,
       count_if(collision_severity = 1) as fatal, count_if(collision_severity = 2) as serious,
       sum(number_of_casualties) as casualties
from incoming.stats19_collisions
where collision_year >= '2015'
  and location_easting_osgr between 448000 and 456000
  and location_northing_osgr between 165000 and 169500
group by 1 order by 1
```

On the A4 (`first_road_class = 3 and first_road_number = 4`), with casualties by type:

```sql
select c.collision_year, k.casualty_type, count(*) as casualties
from incoming.stats19_collisions c
join incoming.stats19_casualties k on k.collision_index = c.collision_index and k.collision_year = c.collision_year
where c.collision_year >= '2015'
  and c.location_easting_osgr between 448000 and 456000 and c.location_northing_osgr between 165000 and 169500
  and c.first_road_class = 3 and c.first_road_number = 4
group by 1, 2 order by 1, 3 desc
```

Around Thatcham there were 18 collisions in 2020 and 36 in 2025, with none fatal over 2020–2025. Of 328 collisions from 2015 to 2025, 134 were on the A4.

## Source ETL / code

`github.com/dantelore/gov-etl`, `stats19/stats19_load.py`. It streams each CSV (about 1.5 GB) and writes one parquet file per year. Column types are fixed in the script: a new column that isn't a whole-number code fails the load rather than drifting.

```bash
python stats19/stats19_load.py                 # all three tables
python stats19/stats19_load.py --table casualty
```

## Freshness & update cadence

Annual. DfT publishes the previous year's final data each September, and may revise earlier years. Loaded 2026-11-12 with 1979 to 2025. Rerun the loader after each release: it rewrites every year.

## Running costs (estimated)

From the S3 size on 2026-11-12. Method: [gov-etl README](README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 1.64 GB, 141 objects (collisions 0.61, vehicles 0.57, casualties 0.46) | $0.038 |
| **Total** | | **$0.038** |

Querying it: an area query over ten years of collisions reads about 25 MB (location columns only).

## Known issues & caveats

- **Injury collisions reported to the police only.** Many slight injuries, and most cyclist injuries, are never reported; damage-only collisions aren't recorded at all.
- **Severity reporting changed.** Forces that moved to the injury-based CRASH and COPA systems (from 2012, at different dates by force) record more injuries as serious. Compare serious casualties across years using the adjusted severity columns, as DfT does, not raw `collision_severity`.
- Latitude and longitude are blank before 1999; use the OSGB columns. Some old locations are rounded or wrong.
- Coordinate columns keep DfT's names (`location_easting_osgr`, `location_northing_osgr`), not the gov-etl `x_coordinate`/`y_coordinate` convention, and the tables are partitioned by year rather than grid tile so the vehicles and casualties partitions line up with their collisions.
- Codes changed over time (the `_historic` columns); check the data guide before comparing a coded field across decades.
- Grid boxes like the one above take in roads outside the town (here, parts of the A4 east and west of Thatcham). Use LSOA or a polygon test for precise boundaries.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|
| Thatcham analysis (thatchamplan.co.uk) | Dan | Road safety on the A4, Floral Way, Harts Hill and the lanes north of the site |

## Status

Active.
