# OpenStreetMap Roads, Paths and Route Features, England

## Description

The OpenStreetMap road and path network for England, for testing walking and cycling routes. Every way tagged `highway=*` (motorways to footpaths: roads, cycleways, footways, bridleways, paths, tracks, steps) with its cycling and walking tags, the OSM node IDs along it (so it can be built into a routable network) and its geometry. Plus the point features that matter on a route: crossings, traffic signals, give-ways, barriers (bollards, gates, cycle barriers), level crossings, cycle parking and hire, and schools and colleges.

| Table | One row per | Rows |
|---|---|---|
| `incoming.osm_paths` | Way | 6,588,457 |
| `incoming.osm_points` | Node, or area (cycle parking, schools) at its centre | 1,433,988 |

Data as of 2026-11-11.

## Classification

Public. Crowd-sourced map data; no personal data.

## Owner

Dan. See the gov-etl repo for the code.

## Source & references

OpenStreetMap, via Geofabrik's daily England extract: https://download.geofabrik.de/europe/united-kingdom/england.html. Tag meanings: https://wiki.openstreetmap.org/wiki/Key:highway, https://wiki.openstreetmap.org/wiki/Key:cycleway, https://wiki.openstreetmap.org/wiki/Key:crossing and the UK tagging guidelines https://wiki.openstreetmap.org/wiki/United_Kingdom_Tagging_Guidelines.

## License & usage restrictions

Open Database Licence 1.0 (ODbL). Attribute "© OpenStreetMap contributors". A database derived from it and shared publicly must also be under the ODbL; maps and charts made from it ("produced works") only need the attribution.

## Location

- S3: `s3://dantelore.data.incoming/osm/paths/grid_e=<e>/grid_n=<n>/osm_paths_<e>_<n>.parquet` and `s3://dantelore.data.incoming/osm/points/grid_e=<e>/grid_n=<n>/osm_points_<e>_<n>.parquet`
- Glue: `incoming.osm_paths`, `incoming.osm_points`
- Format: Parquet (zstd), partitioned by `grid_e` and `grid_n`: the 100 km OSGB square of the way's first node, or the point. Thatcham and Newbury are in `grid_e = 4 and grid_n = 1`. A way that crosses into another square is stored in its first node's square only.

## Field spec

### osm_paths

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| osm_id | bigint | No | Primary | OSM way ID (`https://www.openstreetmap.org/way/<osm_id>`) |
| highway | string | No | | `motorway`, `trunk`, `primary`, `secondary`, `tertiary`, `unclassified`, `residential`, `service`, `living_street`, `cycleway`, `footway`, `path`, `bridleway`, `track`, `steps`, `pedestrian`, `_link` roads, … |
| name, ref | string | Yes | | Street name; road number (`A4`) |
| cycleway, cycleway_left, cycleway_right, cycleway_both | string | Yes | | Cycle provision on a road: `lane` (painted), `track` (separate track alongside), `separate` (drawn as its own way), `shared_lane`, `share_busway`, `no` |
| bicycle, foot | string | Yes | | Access: `designated` (signed for), `yes`, `permissive`, `no`, `dismount` |
| segregated | string | Yes | | On shared paths: `yes` (separate halves for walking and cycling) or `no` |
| surface, smoothness | string | Yes | | E.g. `asphalt`, `gravel`, `grass` |
| lit | string | Yes | | Street lighting, `yes`/`no` |
| maxspeed | string | Yes | | E.g. `30 mph`, `20 mph` |
| oneway, oneway_bicycle | string | Yes | | `yes`, `-1` (against the drawing direction), `no`; `oneway_bicycle = no` lets bikes go both ways |
| access, motor_vehicle | string | Yes | | General and motor access (`private`, `no`, `destination`) |
| width, incline | string | Yes | | As mapped (rarely) |
| bridge, tunnel, layer | string | Yes | | Structures (`bridge = yes`, `tunnel = culvert`, …) |
| sidewalk | string | Yes | | Pavements on roads: `both`, `left`, `right`, `separate`, `no` |
| junction, area | string | Yes | | `roundabout`; `area = yes` for pedestrian squares |
| tags_json | string | No | | Every tag on the way, as JSON |
| node_ids | array<bigint> | No | | The way's nodes in order. Ways that share a node ID meet there |
| length_m | double | No | | Length in metres |
| start_lon, start_lat, end_lon, end_lat | double | No | | First and last node (WGS84) |
| min_lon, min_lat, max_lon, max_lat | double | No | | Bounding box, for filtering an area |
| geometry_wkt | string | No | | `LINESTRING(lon lat, …)`, one point per node, matching `node_ids` |
| snapshot_date | string | No | | Date of the OSM data |
| grid_e, grid_n | int | No | Partition keys | 100 km OSGB square of the first node |

### osm_points

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| osm_id | bigint | No | Primary (with osm_type) | OSM node or way ID |
| osm_type | string | No | | `node`, or `way` for areas (cycle shelters, school grounds) placed at the mean of their outline |
| feature | string | No | | Why it's here: `crossing`, `traffic_signals`, `give_way`, `stop`, `mini_roundabout`, `level_crossing`, `barrier`, `bicycle_parking`, `bicycle_rental`, `bicycle_repair_station`, `school`, `college` |
| highway, crossing, crossing_ref, barrier, railway, amenity | string | Yes | | E.g. `crossing = traffic_signals` (signal-controlled), `uncontrolled` (marked, no signals; `crossing_ref = zebra` for zebras), `unmarked`; `crossing_ref = toucan` (shared with bikes); `barrier = bollard`, `cycle_barrier`, `gate`, `kissing_gate`, `stile` |
| bicycle, foot, kerb | string | Yes | | Access at the point; kerb type at crossings (`lowered`, `flush`, `raised`) |
| capacity, covered, bicycle_parking | string | Yes | | Cycle parking: spaces, shelter, type (`stands`, `wall_loops`, …) |
| name | string | Yes | | |
| tags_json | string | No | | Every tag, as JSON |
| lon, lat | double | No | | WGS84 |
| snapshot_date | string | No | | Date of the OSM data |
| grid_e, grid_n | int | No | Partition keys | 100 km OSGB square |

## Common queries

Cycleways and shared paths in Thatcham, with surface and lighting:

```sql
select osm_id, highway, name, bicycle, segregated, surface, lit, round(length_m) as metres, geometry_wkt
from incoming.osm_paths
where grid_e = 4 and grid_n = 1
  and min_lon > -1.30 and max_lon < -1.20 and min_lat > 51.39 and max_lat < 51.42
  and (highway = 'cycleway' or bicycle in ('designated', 'yes') or cycleway in ('lane', 'track'))
```

Crossings on the A4 through Thatcham, by type:

```sql
select coalesce(crossing, feature) as type, crossing_ref, count(*)
from incoming.osm_points
where grid_e = 4 and grid_n = 1 and feature in ('crossing', 'traffic_signals')
  and lon between -1.28 and -1.21 and lat between 51.398 and 51.408
group by 1, 2 order by 3 desc
```

Kilometres of road with a 20 mph limit, by highway type, in the square:

```sql
select highway, round(sum(length_m) / 1000) as km
from incoming.osm_paths
where grid_e = 4 and grid_n = 1 and maxspeed = '20 mph'
group by 1 order by 2 desc
```

## Source ETL / code

`github.com/dantelore/gov-etl`, `osm/osm_load.py`. Downloads the England extract (1.7 GB), reads it with pyosmium (filtering in C++ so Python only sees tagged objects; node locations are resolved for every way), and writes one Parquet file per grid square per table. Takes about 30 minutes, most of it the download; peaks at about 3.5 GB of memory.

```bash
python osm/osm_load.py
python osm/osm_load.py --pbf /path/to/england-latest.osm.pbf   # reuse a downloaded extract
```

Derived table: [osm_path_gradients](osm_path_gradients.md) (gradients from the lidar).

## Freshness & update cadence

Manual. Geofabrik updates the extract daily; rerun to refresh (each load replaces both tables, deleting the previous files first). Then rerun [osm_path_gradients](osm_path_gradients.md).

## Running costs (estimated)

From the S3 size on 2026-11-13. Method: [gov-etl README](README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 1.254 GB, 54 objects | $0.029 |
| **Total** | | **$0.029** |

Querying it: the Thatcham square's paths file is about 110 MB; select only the columns you need (`tags_json` and `geometry_wkt` are most of the size).

## Known issues & caveats

- **Only as good as the mapping.** OSM is edited by volunteers. Cycle access, segregation, crossings and speed limits are often missing or out of date, and new estates and schemes take time to appear. Check anything that matters on the ground or against aerial imagery before relying on it.
- **Missing isn't "no".** A path with no `bicycle` tag may or may not allow cycling (public footpaths don't; many urban paths are permissive). A road with no `maxspeed` has its default limit, not "unknown speed".
- Crossings are sometimes mapped as a node on the road, sometimes as a separate short `footway = crossing` way, and sometimes both; count carefully.
- Sidewalks drawn as separate ways (`sidewalk = separate`) appear as footways in their own right.
- Ways are split wherever a tag changes, so a street is usually several rows; group by `name` for street totals.
- Points don't include bus stops or stations (see [naptan_stops](naptan_stops.md)) or route relations (National Cycle Network routes).

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|
| Thatcham analysis (thatchamplan.co.uk) | Dan | Cycle network: paths, crossings, gaps |

## Status

Active.
