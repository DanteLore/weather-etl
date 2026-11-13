# Propensity to Cycle Tool (PCT) Outputs, England and Wales

## Description

The Propensity to Cycle Tool (pct.bike) is the model most English councils use for cycle network plans (LCWIPs), including West Berkshire. For every area, home-to-work pair, school and stretch of road it gives how many people cycle now and how many would under each scenario: the Government Target (two versions), Gender Equality, Go Dutch (cycling at Dutch levels, given distance and hilliness) and E-bikes for commuting; the Government Target, Go Cambridge and Go Dutch for school travel. "Now" is the 2011 Census (commuting, England and Wales) and the 2011 School Census (state schools, England only).

| Table | One row per | Rows |
|---|---|---|
| `incoming.pct_commute_zones` | MSOA or LSOA of residence, England and Wales | 7,201 MSOAs, 34,753 LSOAs |
| `incoming.pct_commute_od` | Home-work pair: MSOA pairs in England and Wales; LSOA pairs in the Berkshire region only | 739,271 MSOA, 33,375 LSOA |
| `incoming.pct_school_zones` | LSOA of residence (school travel), England | 32,844 |
| `incoming.pct_schools` | State school in England, with location | 21,443 |
| `incoming.pct_route_network` | Road or path segment, with cyclists now and under each scenario, as a WKT line (commute: England and Wales; school: England) | 558,173 commute, 489,991 school |

## Classification

Public. Modelled and aggregated data; no personal data.

## Owner

Dan. See the gov-etl repo for the code.

## Source & references

- Data: github.com/npct/pct-outputs-national (zones, MSOA pairs, schools, route networks) and github.com/npct/pct-outputs-regional-notR (Berkshire LSOA pairs), Git LFS, last built May 2020.
- Tool and manual: https://www.pct.bike/ (manual: https://www.pct.bike/manual.html). Column definitions: the codebooks in github.com/npct/pct-shiny under `regions_www/www/static/02_codebooks/`. Method: Lovelace et al. (2017), "The Propensity to Cycle Tool: An open source online system for sustainable transport planning", Journal of Transport and Land Use 10(1).

## License & usage restrictions

No separate licence is published for the data files. The PCT is open source (Affero GPL) and offers the downloads freely; it asks users to cite Lovelace et al. (2017) and/or Goodman et al. (2019). Its inputs are Census and DfT data (Open Government Licence) and routes from CycleStreets on OpenStreetMap (© OpenStreetMap contributors, ODbL), so attribute those too.

## Location

- S3: `s3://dantelore.data.incoming/pct_commute_zones/geography_level=<msoa|lsoa>/…`, `pct_commute_od/geography_level=<msoa|lsoa>/…`, `pct_school_zones/pct_school_zones.parquet`, `pct_schools/pct_schools.parquet`, `pct_route_network/purpose=<commute|school>/…`
- Glue: `incoming.pct_commute_zones`, `incoming.pct_commute_od`, `incoming.pct_school_zones`, `incoming.pct_schools`, `incoming.pct_route_network`

## Field spec

Columns are as the PCT publishes them (lower-cased). Most are `<scenario>_<measure>`; all numbers are doubles.

**Scenarios** (names from the PCT's codebooks; definitions in its manual): `govtarget` Government Target (equality), `govnearmkt` Government Target (near market), `gendereq` Gender Equality, `dutch` Go Dutch, `ebike` E-bikes; school files have `govtarget`, `cambridge` (Go Cambridge) and `dutch`.

**Measures:** `slc` cyclists under the scenario; `sic` increase in cyclists against 2011; `slw`/`siw` walkers; `sld`/`sid` car drivers; `slp`/`sip` car passengers; `slm`/`sim` motorcyclists; `slpt`/`sipt` public transport users; `sicyclehours` change in cycling hours a year; `sideath`, `siyll`, `sisickdays` change in deaths, years of life lost and sick days a year; `sivalueyll`, `sivaluesick`, `sivaluecomb` their value in £; `sicarkm` change in car driver km a year and `sico2` change in CO2 (kg a year). `base_sl…` columns compare 2011 with no cycling.

### pct_commute_zones and pct_school_zones

| Field name | Type | Description |
|---|---|---|
| geo_code, geo_name | string | 2011 MSOA or LSOA code and name |
| lad11cd, lad_name | string | Local authority (2011) |
| all | double | Commuters (2011 Census, excluding home workers) / school pupils living in the area |
| bicycle, foot, car_driver, car_passenger, motorbike, train_tube, bus, taxi_other | double | Commuters by main mode in 2011 (school files: bicycle, foot, car) |
| <scenario>_<measure> | double | As above |
| perc_rf_dist_u10km (commute), perc_rf_dist_u5km (school) | double | % of trips under 10 km / 5 km by the fastest cycle route |
| avslope_perc_u10km, avslope_perc_u5km | double | Average gradient (%) of those routes: how hilly cycling is from the area |
| baseline_at_mmet, base_slmmet, …_simmet | double | School files: physical activity (marginal MET hours) now and under each scenario |
| geography_level | string | Partition key (commute zones): `msoa` or `lsoa` |

### pct_commute_od

As the zones, plus:

| Field name | Type | Description |
|---|---|---|
| id | string | `<geo_code1> <geo_code2>` |
| geo_code1, geo_name1, lad11cd1, lad_name1 | string | Home area |
| geo_code2, geo_name2, lad11cd2, lad_name2 | string | Workplace area |
| e_dist_km | double | Straight-line distance |
| rf_dist_km, rq_dist_km | double | Fastest and quietest cycle route distance (CycleStreets) |
| dist_rf_e, dist_rq_rf | double | Route circuity: fast route ÷ straight line, quiet route ÷ fast route |
| rf_avslope_perc, rq_avslope_perc | double | Average gradient of the fast and quiet routes |
| rf_time_min, rq_time_min | double | Cycling time by each route |
| geography_level | string | Partition key: `msoa` (England and Wales) or `lsoa` (Berkshire region) |

### pct_schools

`urn` (school URN), `schoolname`, `phase` (`Primary`, `Secondary`), `secondary` (1/0), `lsoa11cd`, `lsoa11nm`, `lad11cd`, `lad_name`, `all` (pupils), `bicycle`, `foot`, `car`, the school scenario columns, `perc_rf_dist_u5km`, `avslope_perc_u5km`, and `longitude`, `latitude` (WGS84).

### pct_route_network

| Field name | Type | Description |
|---|---|---|
| local_id | bigint | Segment ID within its network |
| bicycle | double | Between-zone cyclists using the segment in 2011 (commuters or pupils) |
| govtarget_slc, govnearmkt_slc, gendereq_slc, dutch_slc, ebike_slc, cambridge_slc | double | Cyclists under each scenario (null where the scenario doesn't apply to the purpose) |
| min_lon, min_lat, max_lon, max_lat | double | Bounding box, for filtering an area |
| geometry_wkt | string | `LINESTRING(lon lat, …)` in WGS84 |
| purpose | string | Partition key: `commute` or `school` |

## Common queries

Cycling potential for Thatcham's MSOAs:

```sql
select geo_name, "all" as commuters, bicycle as cyclists_2011, govtarget_slc, gendereq_slc, dutch_slc, ebike_slc, avslope_perc_u10km
from incoming.pct_commute_zones
where geography_level = 'msoa' and geo_code in ('E02003377', 'E02003380', 'E02003382', 'E02003383')
```

Schools near Thatcham, cycling now and under Go Dutch:

```sql
select schoolname, phase, "all" as pupils, bicycle, govtarget_slc, cambridge_slc, dutch_slc
from incoming.pct_schools
where longitude between -1.30 and -1.21 and latitude between 51.39 and 51.42
order by pupils desc
```

Busiest Go Dutch commuting segments in Thatcham (map `geometry_wkt`):

```sql
select local_id, bicycle, dutch_slc, ebike_slc, geometry_wkt
from incoming.pct_route_network
where purpose = 'commute' and min_lon > -1.30 and max_lon < -1.21 and min_lat > 51.39 and max_lat < 51.42
order by dutch_slc desc limit 50
```

## Source ETL / code

`github.com/dantelore/gov-etl`, `pct/pct_load.py`. Downloads each file from GitHub (about 1.1 GB in all), keeps every column, and writes Parquet; route network and school GeoJSON are flattened to WKT and coordinates.

```bash
python pct/pct_load.py
python pct/pct_load.py --part route_networks   # or tables, schools
```

## Freshness & update cadence

Static. The PCT outputs were last built in May 2020 from 2011 data; loaded 2026-11-12. A PCT rebuild on Census 2021 hadn't been published when this was loaded; reload if it is.

## Running costs (estimated)

From the S3 size on 2026-11-12. Method: [gov-etl README](README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.265 GB, 8 objects | $0.006 |
| **Total** | | **$0.006** |

## Known issues & caveats

- **"Now" is 2011.** Commuting baselines are the 2011 Census and school baselines the 2011 School Census, on 2011 areas; the scenarios are uplifts on those. New estates built since (e.g. north-east Thatcham) don't exist in the model.
- **Not a forecast.** The scenarios show what cycling could be if the conditions assumed were met (e.g. Dutch-quality infrastructure and culture), given each trip's distance and hilliness. They are a measure of potential, not a prediction for a scheme.
- Commuting covers only home-work pairs within 30 km by the fastest route (plus trips within each zone); `pct_commute_od` totals 23.9 million commuters, the same as `pct_commute_zones`.
- The LSOA pairs are loaded for the Berkshire region only: the national LSOA pairs file is 1.2 GB on the PCT's GitHub LFS, which counts against the project's download allowance. MSOA pairs are national.
- Route network values count **between-zone** cyclists only (trips within a zone aren't routed), from the model's route assignment; a segment's figure includes everyone routed along it, not only people living nearby. Routes are CycleStreets' "fast" routes on the OpenStreetMap of the time. `local_id`s repeat between the commute and school networks.
- School data covers state schools in England only; independent schools are missing.
- Zones use 2011 codes; Thatcham's MSOAs kept theirs in 2021, but some LSOAs changed (see [ons_census_area_lookup_2011_2021](ons_census_area_lookup_2011_2021.md)).
- The school files publish two columns both named `dutch_sicarkm`; the second is loaded as `dutch_sicarkm_1` (it sits where the Cambridge scenario's car km would be, so it is probably that, mislabelled).

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|
| Thatcham analysis (thatchamplan.co.uk) | Dan | What cycling could be in Thatcham, commuting and school trips |

## Status

Active.
