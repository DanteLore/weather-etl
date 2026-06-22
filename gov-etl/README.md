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
