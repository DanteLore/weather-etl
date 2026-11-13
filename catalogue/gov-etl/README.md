# gov-etl

Datasets produced by the [gov-etl](https://github.com/dantelore/gov-etl) pipeline. Open government data from UK public bodies — ONS, OS, HM Land Registry, Environment Agency, DfT, VOA, and others. All datasets are public and licensed under the Open Government Licence unless noted otherwise in the individual entry.

All datasets land in S3 under `s3://dantelore.data.incoming/` and are queryable via Athena (`incoming` database).

## Load cadence

All datasets in this folder are loaded **manually and on an ad-hoc basis**. There are no
scheduled jobs or automatic refreshes. To reload a dataset, run its loader script directly
— see the individual entry for the exact command. Check the source agency's release page
before re-running if you need the latest data.

## Spatial partitioning convention

Point and raster datasets use a shared two-key partition scheme based on 100km OSGB36 grid squares:

| Partition key | Derivation | Range |
|---|---|---|
| `grid_e` | `floor(x_coordinate / 100000)` | 0–6 |
| `grid_n` | `floor(y_coordinate / 100000)` | 0–12 |

Coordinate columns are `x_coordinate` (easting) and `y_coordinate` (northing) in OSGB36 metres (EPSG:27700). Always filter on both partition keys to get pruning. Postcode-anchored datasets use `postcode_area` (e.g. `rg`) instead.

## Datasets

| Dataset | Table | Description |
|---|---|---|
| [house_prices](house_prices.md) | `incoming.house_prices_ppd` | Every residential property sale in England & Wales since 1995 |
| [voa_rating_list](voa_rating_list.md) | `incoming.voa_rating_list_entries` | 2.14M non-domestic properties from the VOA 2026 rating list |
| [ons_postcode_lookup](ons_postcode_lookup.md) | `incoming.ons_postcode_lookup_lookup` | ~2.35M postcodes mapped to MSOA, LAD, region and country |
| [os_code_point_open](os_code_point_open.md) | `incoming.os_code_point_open_codepo` | OSGB coordinates for ~1.8M GB postcodes |
| [os_open_uprn](os_open_uprn.md) | `incoming.os_open_uprn_uprn` | Coordinates for ~42M address points (UPRNs) across GB |
| [ons_bua_boundaries](ons_bua_boundaries.md) | `incoming.ons_bua_boundaries_bua` | Polygon boundaries for 7,775 Built-up Areas in England & Wales |
| [ons_ctyua_boundaries](ons_ctyua_boundaries.md) | `incoming.ons_ctyua_boundaries_ctyua` | Polygon boundaries for 205 Counties and Unitary Authorities (GB) |
| [ons_msoa_boundaries](ons_msoa_boundaries.md) | `incoming.ons_msoa_boundaries_msoa` | Polygon boundaries for 7,264 MSOAs in England & Wales |
| [ons_msoa_income](ons_msoa_income.md) | `incoming.ons_msoa_income_income` | Modelled household income estimates by MSOA (FYE 2023) |
| [ons_inflation](ons_inflation.md) | `incoming.ons_inflation_inflation` | Monthly CPI, CPIH and RPI series from Jun 1948 to present |
| [traffic_census](traffic_census.md) | `incoming.traffic_census_aadf` | Annual average daily vehicle flows, 2000–present, all GB roads |
| [nomis_census_industry](nomis_census_industry.md) | `incoming.nomis_census_industry` | Census 2021 employment by industry sector, all wards in England & Wales |
| [nomis_census_occupation](nomis_census_occupation.md) | `incoming.nomis_census_occupation` | Census 2021 employment by occupation group, all wards in England & Wales |
| [os_terrain_50](os_terrain_50.md) | `incoming.os_terrain_50_terrain50` | 50m DTM elevation for all of GB (~114M points) |
| [ea_lidar_1m](ea_lidar_1m.md) | `incoming.ea_lidar_1m_dtm` | 1m DTM elevation for England (selected areas — currently West Berkshire) |
| [exchange_rates](exchange_rates.md) | `incoming.exchange_rates_frankfurter` | Daily ECB reference rates for 46 currencies vs EUR, 1999–present |
| [world_bank_gdp](world_bank_gdp.md) | `incoming.world_bank_gdp` | World Bank GDP indicators for all countries, 1960 onwards |
| [nomis_census_travel_method](nomis_census_travel_method.md) | `incoming.nomis_census_travel_method` | Census 2021 method of travel to work, by OA, LSOA, MSOA, local authority and ward |
| [nomis_census_travel_distance](nomis_census_travel_distance.md) | `incoming.nomis_census_travel_distance` | Census 2021 distance travelled to work, same geographies |
| [nomis_census_car_availability](nomis_census_car_availability.md) | `incoming.nomis_census_car_availability` | Census 2021 cars or vans per household, same geographies |
| [nomis_census_commute_flows](nomis_census_commute_flows.md) | `incoming.nomis_census_commute_flows` | Census 2021 home-to-workplace flows between output areas, MSOAs and local authorities |
| [stats19](stats19.md) | `incoming.stats19_collisions`, `_vehicles`, `_casualties` | Every reported injury road collision in GB since 1979, with vehicles and casualties |
| [traffic_census_raw_counts](traffic_census_raw_counts.md) | `incoming.traffic_census_raw_counts` | Hourly manual traffic counts by vehicle type on count days, 2000–present |
| [naptan_stops](naptan_stops.md) | `incoming.naptan_stops` | Every public transport stop in GB (436k, incl. inactive) with location |
| [dft_journey_times_lsoa](dft_journey_times_lsoa.md) | `incoming.dft_journey_times_lsoa` | Modelled travel times from each LSOA in England to key services (2019 edition, the latest) |
| [dft_bus_statistics_la](dft_bus_statistics_la.md) | `incoming.dft_bus_statistics_la` | Local bus journeys, vehicle miles and punctuality by local authority, annual |
| [webtris](webtris.md) | `incoming.webtris_counts`, `_daily_flows`, `_hourly_profiles`, `_sites` | Daily flows and hourly profiles at every WebTRIS site in England, plus 15-minute counts and speeds on the M4 (J12–J14) and A34 near Newbury, 2017 onwards |
| [street_manager_permit_events](street_manager_permit_events.md) | `incoming.street_manager_permit_events` | Roadworks permit and works events for every highway authority in England, July 2020 onwards |
| [dft_walking_cycling_la](dft_walking_cycling_la.md) | `incoming.dft_walking_cycling_la` | Share of adults walking and cycling by purpose and frequency, every local authority in England, 2016 onwards |
| [nomis_census_2011_travel](nomis_census_2011_travel.md) | `incoming.nomis_census_2011_travel_method`, `_commute_flows` | Census 2011 method of travel to work (OA to local authority) and home-to-work flows by method (MSOA, local authority) |
| [ons_census_area_lookup_2011_2021](ons_census_area_lookup_2011_2021.md) | `incoming.ons_census_area_lookup_2011_2021` | How 2011 output areas, LSOAs and MSOAs map to 2021 ones |
| [dft_national_travel_survey](dft_national_travel_survey.md) | `incoming.dft_nts_tables`, `incoming.dft_ntas_responses` | Every National Travel Survey table, and National Travel Attitudes Study waves 7–10 |
| [pct](pct.md) | `incoming.pct_commute_zones`, `_commute_od`, `_school_zones`, `_schools`, `_route_network` | Propensity to Cycle Tool: cycling now (2011) and under scenarios, by area, home-work pair, school and road segment |
| [osm](osm.md) | `incoming.osm_paths`, `incoming.osm_points` | OpenStreetMap roads and paths (6.6M ways) with cycling tags and node IDs, plus crossings, barriers and cycle parking, England |
| [osm_path_gradients](osm_path_gradients.md) | `incoming.osm_path_gradients` | Climb and steepest 50 m of every road and path where the 1m lidar is loaded |
| [osm_cycle_routes](osm_cycle_routes.md) | `incoming.osm_cycle_routes`, `_route_segments`, `_network_ends` | Cycle route tests from the NE Thatcham site to the station, Kennet School and town centre; where Thatcham's cycle paths stop |

## Running costs

Estimated from measurements taken 2026-11-05, at AWS eu-west-1 list prices in USD, excluding tax and the free tier:

| Component | Basis | Price |
|---|---|---|
| S3 storage | Size of each table's S3 location | $0.023 per GB-month |
| S3 writes / reads | New files and Athena result files; objects read by nightly rebuilds | $0.005 per 1,000 PUTs; $0.0004 per 1,000 GETs |
| Lambda | Invocations × average duration × memory (CloudWatch, last 30 days) | $0.0000166667 per GB-second + $0.20 per million requests |
| Athena | Bytes scanned by each pipeline query (Athena query history, last 30 days); 10 MB minimum per query; DDL such as `ADD PARTITION` is free | $5 per TB scanned |
| CloudWatch Logs | Log ingestion per Lambda (CloudWatch, last 30 days) | $0.57 per GB |

Lambdas that load several tables are split evenly between them; a model Lambda is split by bytes scanned per table. The free tier currently covers all Lambda usage, so Lambda is billed at about $0 in practice. Each entry's **Running costs (estimated)** section has the per-table breakdown.

| Dataset | Layer | Est. $/month |
|---|---|---|
| [ea_lidar_1m](ea_lidar_1m.md) | incoming | $0.091 |
| [house_prices](house_prices.md) | incoming | $0.037 |
| [os_open_uprn](os_open_uprn.md) | incoming | $0.025 |
| [ons_bua_boundaries](ons_bua_boundaries.md) | incoming | $0.005 |
| [os_terrain_50](os_terrain_50.md) | incoming | $0.004 |
| [voa_rating_list](voa_rating_list.md) | incoming | $0.003 |
| [traffic_census](traffic_census.md) | incoming | $0.001 |
| [os_code_point_open](os_code_point_open.md) | incoming | $0.001 |
| [ons_postcode_lookup](ons_postcode_lookup.md) | incoming | <$0.001 |
| [ons_msoa_boundaries](ons_msoa_boundaries.md) | incoming | <$0.001 |
| [exchange_rates](exchange_rates.md) | incoming | <$0.001 |
| [ons_ctyua_boundaries](ons_ctyua_boundaries.md) | incoming | <$0.001 |
| [ons_msoa_income](ons_msoa_income.md) | incoming | <$0.001 |
| [world_bank_gdp](world_bank_gdp.md) | incoming | <$0.001 |
| [nomis_census_industry](nomis_census_industry.md) | incoming | <$0.001 |
| [nomis_census_occupation](nomis_census_occupation.md) | incoming | <$0.001 |
| [ons_inflation](ons_inflation.md) | incoming | <$0.001 |
| [stats19](stats19.md) | incoming | $0.038 |
| [webtris](webtris.md) | incoming | $0.010 |
| [traffic_census_raw_counts](traffic_census_raw_counts.md) | incoming | $0.002 |
| [street_manager_permit_events](street_manager_permit_events.md) | incoming | $0.177 |
| [naptan_stops](naptan_stops.md) | incoming | $0.001 |
| [dft_journey_times_lsoa](dft_journey_times_lsoa.md) | incoming | $0.001 |
| [nomis_census_travel_method](nomis_census_travel_method.md) | incoming | <$0.001 |
| [nomis_census_travel_distance](nomis_census_travel_distance.md) | incoming | <$0.001 |
| [nomis_census_car_availability](nomis_census_car_availability.md) | incoming | <$0.001 |
| [nomis_census_commute_flows](nomis_census_commute_flows.md) | incoming | $0.002 |
| [dft_bus_statistics_la](dft_bus_statistics_la.md) | incoming | <$0.001 |
| [dft_walking_cycling_la](dft_walking_cycling_la.md) | incoming | <$0.001 |
| [nomis_census_2011_travel](nomis_census_2011_travel.md) | incoming | <$0.001 |
| [ons_census_area_lookup_2011_2021](ons_census_area_lookup_2011_2021.md) | incoming | <$0.001 |
| [dft_national_travel_survey](dft_national_travel_survey.md) | incoming | <$0.001 |
| [pct](pct.md) | incoming | $0.006 |
| [osm](osm.md) | incoming | $0.029 |
| [osm_path_gradients](osm_path_gradients.md) | incoming | <$0.001 |
| [osm_cycle_routes](osm_cycle_routes.md) | incoming | <$0.001 |
| **Total** | | **$0.43** |

The datasets added on 2026-11-12 and 2026-11-13 (from `stats19` down) were measured on those days.
