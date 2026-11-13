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

## rail-etl / incoming (raw)

| Dataset | Description |
|---|---|
| [incoming.rtt_services](rail-etl/incoming/rtt_services.md) | RTT actual-vs-scheduled times, cancellations and formation at the Newbury–Paddington corridor stations, daily |
| [incoming.rtt_reasons](rail-etl/incoming/rtt_reasons.md) | RTT reason codes and text for corridor services with cancelled or terminated calls |
| [incoming.nr_schedule_locations](rail-etl/incoming/nr_schedule_locations.md) | Network Rail timetable for corridor trains, including planned changes and buses, one snapshot a day |
| [incoming.nr_delay_attribution](rail-etl/incoming/nr_delay_attribution.md) | Network Rail's attributed delays and cancellations to every GB passenger train, with cause and responsibility, plus glossary tables |
| [incoming.hsp_service_calls](rail-etl/incoming/hsp_service_calls.md) | HSP times and reason codes at every stop of Newbury-line trains, with a rolling year of history |
| [incoming.orr_rail_performance](rail-etl/incoming/orr_rail_performance.md) | The 20 ORR passenger rail performance tables, one row per cell, one snapshot per ORR release |
| [incoming.bods_vehicle_activity](rail-etl/incoming/bods_vehicle_activity.md) | Live bus positions in the Thatcham area (Newbury to Theale), polled every minute from the Bus Open Data Service |
| [incoming.bods_gtfs_*](rail-etl/incoming/bods_gtfs.md) | Bus timetables (GTFS) for every journey calling in the Thatcham area, one snapshot a day |

## rail-etl / lake (modelled)

| Dataset | Description |
|---|---|
| [lake.rtt_service_calls](rail-etl/lake/rtt_service_calls.md) | One row per train call at a corridor station, typed and de-duplicated, rebuilt daily |
| [lake.rtt_services](rail-etl/lake/rtt_services.md) | One row per corridor train per day: lateness, cancellations, on time / PPM, short formation, rebuilt daily |
| [lake.nr_schedule_days](rail-etl/lake/nr_schedule_days.md) | Planned timetable changes per corridor train per day: cancelled, cut back, overlaid, new, bus, rebuilt daily |
| [lake.nr_delay_attribution](rail-etl/lake/nr_delay_attribution.md) | Delays and cancellations to corridor trains by cause and responsibility (Network Rail / operator), from February 2026, rebuilt daily |
| [lake.hsp_service_calls](rail-etl/lake/hsp_service_calls.md) | One row per stop of every Newbury-line train, with lateness, cancellations and reason codes, rebuilt daily |
| [lake.hsp_services](rail-etl/lake/hsp_services.md) | One row per Newbury-line train per day across its whole journey, rebuilt daily |
| [lake.orr_operator_cancellations](rail-etl/lake/orr_operator_cancellations.md) | ORR trains planned and cancelled by operator per rail period, rebuilt daily |
| [lake.orr_operator_punctuality](rail-etl/lake/orr_operator_punctuality.md) | ORR on time and "Time to N" at station stops by operator per rail period, rebuilt daily |
| [lake.orr_operator_ppm](rail-etl/lake/orr_operator_ppm.md) | ORR Public Performance Measure by operator and sector per rail period, rebuilt daily |
| [lake.orr_operator_passenger_lateness](rail-etl/lake/orr_operator_passenger_lateness.md) | ORR average passenger lateness by operator and sector per rail period, rebuilt daily |
| [lake.orr_operator_delay_causes](rail-etl/lake/orr_operator_delay_causes.md) | ORR delay minutes by operator, responsibility and cause per rail period, rebuilt daily |
| [lake.orr_sub_operator_performance](rail-etl/lake/orr_sub_operator_performance.md) | ORR right time, PPM and CaSL by sub-operator per rail period, rebuilt daily |
| [lake.orr_severe_disruption_days](rail-etl/lake/orr_severe_disruption_days.md) | ORR days of severe disruption, GB and by sub-operator, rebuilt daily |
| [lake.orr_network_rail_performance](rail-etl/lake/orr_network_rail_performance.md) | ORR cancellations, on time and delay per 1,000 train miles by Network Rail region and route, rebuilt daily |
| [lake.bus_planned_calls](rail-etl/lake/bus_planned_calls.md) | Bus timetable by day: every timetabled journey at every stop in the Thatcham area, rebuilt daily |
| [lake.bus_positions](rail-etl/lake/bus_positions.md) | Bus positions in the Thatcham area, de-duplicated, rebuilt daily |
| [lake.bus_service_calls](rail-etl/lake/bus_service_calls.md) | Bus punctuality: timetabled calls in the Thatcham area with actual pass times and lateness, rebuilt daily |

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
| [nomis_census_travel_method](gov-etl/nomis_census_travel_method.md) | Census 2021 method of travel to work (TS061) by output area, LSOA, MSOA, local authority and ward |
| [nomis_census_travel_distance](gov-etl/nomis_census_travel_distance.md) | Census 2021 distance travelled to work (TS058) by output area, LSOA, MSOA, local authority and ward |
| [nomis_census_car_availability](gov-etl/nomis_census_car_availability.md) | Census 2021 cars or vans per household (TS045) by output area, LSOA, MSOA, local authority and ward |
| [nomis_census_commute_flows](gov-etl/nomis_census_commute_flows.md) | Census 2021 home-to-workplace flows (ODWP01EW) between output areas, MSOAs and local authorities |
| [stats19](gov-etl/stats19.md) | DfT road safety data: every reported injury road collision in GB since 1979, with vehicles and casualties |
| [traffic_census_raw_counts](gov-etl/traffic_census_raw_counts.md) | DfT hourly manual traffic counts by vehicle type on count days, all GB count points, 2000 to present |
| [naptan_stops](gov-etl/naptan_stops.md) | NaPTAN: every bus stop, station and other public transport stop in GB, with location |
| [dft_journey_times_lsoa](gov-etl/dft_journey_times_lsoa.md) | DfT journey time statistics: travel times from each LSOA in England to jobs, schools, GPs, hospitals, shops and town centres (2019 edition) |
| [dft_bus_statistics_la](gov-etl/dft_bus_statistics_la.md) | DfT local bus statistics by local authority: passenger journeys, vehicle miles and punctuality, annual |
| [webtris](gov-etl/webtris.md) | National Highways WebTRIS: daily flows and hourly profiles at every site in England, 15-minute counts on the M4 (J12–J14) and A34 near Newbury |
| [street_manager_permit_events](gov-etl/street_manager_permit_events.md) | DfT Street Manager roadworks permits and works events for every highway authority in England, 2020 onwards |
| [dft_walking_cycling_la](gov-etl/dft_walking_cycling_la.md) | DfT walking and cycling statistics (CW0301–CW0303): share of adults walking and cycling by local authority, yearly from 2016 |
| [nomis_census_2011_travel](gov-etl/nomis_census_2011_travel.md) | Census 2011 method of travel to work (QS701EW) and home-to-workplace flows by method (WU03EW) |
| [ons_census_area_lookup_2011_2021](gov-etl/ons_census_area_lookup_2011_2021.md) | ONS lookups from 2011 to 2021 Census output areas, LSOAs and MSOAs |
| [dft_national_travel_survey](gov-etl/dft_national_travel_survey.md) | DfT National Travel Survey tables and National Travel Attitudes Study responses (waves 7–10) |
| [pct](gov-etl/pct.md) | Propensity to Cycle Tool: cycling to work and school now and under scenarios, by area, home-work pair, school and route segment |
| [osm](gov-etl/osm.md) | OpenStreetMap roads, paths, crossings, barriers and cycle parking for England, with cycling tags and a routable node list |
| [osm_path_gradients](gov-etl/osm_path_gradients.md) | Gradients of OpenStreetMap roads and paths from the EA 1m lidar (West Berkshire and one London tile) |
| [osm_cycle_routes](gov-etl/osm_cycle_routes.md) | Cycle route tests from the North East Thatcham site, and where Thatcham's cycle paths stop |
