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

## energy-etl / incoming (raw)

| Dataset | Description |
|---|---|
| [carbon_intensity_national](energy-etl/incoming/carbon_intensity_national.md) | National UK carbon intensity — forecast, actual and index, half-hourly |
| [carbon_intensity_regional](energy-etl/incoming/carbon_intensity_regional.md) | Per-DNO-region carbon intensity and generation mix, half-hourly |
| [carbon_intensity_generation](energy-etl/incoming/carbon_intensity_generation.md) | National fuel-type generation share (%) per half-hour from the Carbon Intensity API |
| [elexon_system_prices](energy-etl/incoming/elexon_system_prices.md) | Elexon System Buy/Sell Prices and imbalance volume per settlement period |
| [elexon_market_index](energy-etl/incoming/elexon_market_index.md) | Wholesale electricity prices (MID) from N2EX and APX per settlement period |
| [elexon_fuelhh](energy-etl/incoming/elexon_fuelhh.md) | Half-hourly generation by fuel type in MW — the most granular generation mix data |
| [elexon_bod](energy-etl/incoming/elexon_bod.md) | Balancing Mechanism bid and offer prices/levels per BM unit per settlement period |
| [elexon_boalf](energy-etl/incoming/elexon_boalf.md) | Accepted balancing instructions (BOA Level File) per BM unit |
| [elexon_pn](energy-etl/incoming/elexon_pn.md) | Physical Notifications — BM unit planned output profiles before gate closure |
| [elexon_netbsad](energy-etl/incoming/elexon_netbsad.md) | Net Balancing Services Adjustment Data — price adjustments per settlement period |
| [remit](energy-etl/incoming/remit.md) | REMIT outage notifications for UK generation and transmission assets — every revision; use lake.remit for final state |
| [repd](energy-etl/incoming/repd.md) | Renewable Energy Planning Database — all UK renewable/low-carbon projects and their status |
| [natgas_sap](energy-etl/incoming/natgas_sap.md) | National Gas System Average Price (p/kWh) by gas day |
| [eia_brent_crude](energy-etl/incoming/eia_brent_crude.md) | Europe Brent crude oil daily spot price (USD/barrel), weekdays only |

## energy-etl / lake (modelled)

| Dataset | Description |
|---|---|
| [lake.carbon_intensity_national](energy-etl/lake/carbon_intensity_national.md) | National carbon intensity (gCO₂/kWh), forecast and actual, one row per half-hour period, rebuilt nightly |
| [lake.carbon_intensity_regional](energy-etl/lake/carbon_intensity_regional.md) | Regional carbon intensity forecast and generation mix, one row per period and region, rebuilt nightly |
| [lake.carbon_intensity_generation](energy-etl/lake/carbon_intensity_generation.md) | National generation mix (%), including embedded wind and solar, one row per half-hour period, rebuilt nightly |
| [lake.elexon_fuelhh](energy-etl/lake/elexon_fuelhh.md) | Half-hourly generation by fuel type (MW), one row per settlement period, rebuilt nightly |
| [lake.elexon_system_prices](energy-etl/lake/elexon_system_prices.md) | System Buy/Sell Prices and imbalance volume, one row per settlement period, rebuilt nightly |
| [lake.elexon_market_index](energy-etl/lake/elexon_market_index.md) | Wholesale power price per settlement period, volume-weighted across providers, rebuilt nightly |
| [lake.elexon_netbsad](energy-etl/lake/elexon_netbsad.md) | Balancing services price adjustments, one row per settlement period, rebuilt nightly |
| [lake.natgas_sap](energy-etl/lake/natgas_sap.md) | Gas System Average Price (p/kWh), one row per gas day, rebuilt nightly |
| [lake.eia_brent_crude](energy-etl/lake/eia_brent_crude.md) | Brent crude spot price (USD/barrel), one row per trading day, rebuilt nightly |
| [lake.remit](energy-etl/lake/remit.md) | REMIT outage notifications, final state — one row per outage from its last revision, rebuilt nightly |

## analytics-etl

| Dataset | Description |
|---|---|
| [incoming.webanalytics](analytics-etl/incoming/webanalytics.md) | Raw CloudFront access logs for dantelore.com — one row per HTTP request, tab-delimited, partitioned by year/month/day |
| [lake.webanalytics](analytics-etl/lake/webanalytics.md) | Enriched web analytics with geolocation, user-agent parsing, bot detection and content flagging — the main analysis table |

## rail-etl

| Dataset | Description |
|---|---|
| [rtt_services](rail-etl/rtt_services.md) | RTT actual-vs-scheduled times, cancellations and formation for THA/RDG/PAD |

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
| [exchange_rates](gov-etl/exchange_rates.md) | Daily ECB foreign exchange reference rates for 46 currencies (32 still active) vs EUR, from 1999 to present |
| [traffic_census](gov-etl/traffic_census.md) | Annual average daily vehicle flows at ~30k road count points, 2000 to present |
| [nomis_census_industry](gov-etl/nomis_census_industry.md) | Census 2021 employment by industry sector for all wards in England & Wales |
| [nomis_census_occupation](gov-etl/nomis_census_occupation.md) | Census 2021 employment by occupation group for all wards in England & Wales |
| [os_terrain_50](gov-etl/os_terrain_50.md) | 50m-resolution DTM elevation for all of Great Britain (~114M points) |
| [ea_lidar_1m](gov-etl/ea_lidar_1m.md) | 1m-resolution bare-earth DTM for England — currently loaded for West Berkshire |
| [world_bank_gdp](gov-etl/world_bank_gdp.md) | World Bank GDP indicators (current, constant 2015, PPP, per capita) for all countries, 1960 onwards |
