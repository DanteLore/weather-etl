# Data Catalogue Index

One line per dataset. For full details, follow the link.

## weather-etl / incoming (raw)

| Dataset | Description |
|---|---|
| [incoming.weather](weather-etl/incoming/weather.md) | Raw hourly Met Office observations as received from the DataHub API, written every hour |
| [incoming.midas](weather-etl/incoming/midas.md) | Raw historic CEDA MIDAS Open observations (202107 snapshot, one-off load) |

## weather-etl / lake (modelled)

| Dataset | Description |
|---|---|
| [lake.weather](weather-etl/lake/weather.md) | Deduplicated, cleaned hourly Met Office observations — the main analysis table, rebuilt nightly |
| [lake.weather_stations](weather-etl/lake/weather_stations.md) | Metadata for the 137 UK/Ireland Met Office monitoring stations |
| [lake.weather_monthly_site_summary](weather-etl/lake/weather_monthly_site_summary.md) | Percentile-based monthly temperature summary by station, rebuilt nightly |

## gov-etl

| Dataset | Description |
|---|---|
| [house_prices](gov-etl/house_prices.md) | Every residential property sale registered with HM Land Registry in England & Wales since 1995 |
| [voa_rating_list](gov-etl/voa_rating_list.md) | 2.14M non-domestic properties from the VOA 2026 compiled rating list |
| [ons_postcode_lookup](gov-etl/ons_postcode_lookup.md) | ONS lookup mapping ~2.35M England & Wales postcodes to MSOA, local authority, region, and country |
| [os_code_point_open](gov-etl/os_code_point_open.md) | OSGB36 coordinates for ~1.8M GB postcodes |
| [os_open_uprn](gov-etl/os_open_uprn.md) | Coordinates for ~42M address points (UPRNs) across Great Britain |
| [ons_bua_boundaries](gov-etl/ons_bua_boundaries.md) | Polygon boundaries for 7,775 Built-up Areas in England & Wales (2024) |
| [ons_ctyua_boundaries](gov-etl/ons_ctyua_boundaries.md) | Polygon boundaries for 205 Counties and Unitary Authorities across Great Britain |
| [ons_msoa_boundaries](gov-etl/ons_msoa_boundaries.md) | Polygon boundaries for 7,264 MSOAs in England & Wales with Rural-Urban Classification |
| [ons_msoa_income](gov-etl/ons_msoa_income.md) | Modelled household income estimates by MSOA, financial year ending March 2023 |
| [ons_inflation](gov-etl/ons_inflation.md) | Monthly CPI, CPIH and RPI inflation series from June 1948 to present |
| [traffic_census](gov-etl/traffic_census.md) | Annual average daily vehicle flows at ~30k road count points, 2000 to present |
| [nomis_census_industry](gov-etl/nomis_census_industry.md) | Census 2021 employment by industry sector for all wards in England & Wales |
| [nomis_census_occupation](gov-etl/nomis_census_occupation.md) | Census 2021 employment by occupation group for all wards in England & Wales |
| [os_terrain_50](gov-etl/os_terrain_50.md) | 50m-resolution DTM elevation for all of Great Britain (~114M points) |
| [ea_lidar_1m](gov-etl/ea_lidar_1m.md) | 1m-resolution bare-earth DTM for England — currently loaded for West Berkshire |
