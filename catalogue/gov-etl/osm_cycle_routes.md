# Thatcham Cycle Route Tests (OpenStreetMap)

## Description

Tests of cycling routes from the North East Thatcham allocation (Local Plan policy SP14) to Thatcham station, Kennet School and the town centre (the Broadway), on today's OpenStreetMap network. For each route: how long it is, how much it climbs there and back, how much of it is on cycleways, shared paths and quiet streets, which main roads it crosses without a mapped crossing, and the longest stretch without low-stress provision. Plus a list of every place in the Thatcham area where a cycleway or shared path stops. Built from [osm](osm.md) and [osm_path_gradients](osm_path_gradients.md).

| Table | One row per | Rows |
|---|---|---|
| `incoming.osm_cycle_routes` | Origin × destination × route type | 252 (28 origins × 3 destinations × 3 route types) |
| `incoming.osm_cycle_route_segments` | Way used by a route, in order | 5,985 |
| `incoming.osm_cycle_network_ends` | Place where a cycleway, shared path or cycle track ends | 147 |

**Origins** are points every 250 m across the SP14 boundary, each joined to the nearest point on today's network. The site is mostly fields, so the median join is about 125 m (`snap_m`); the routes describe the network the site's own streets would connect to.

**Route types:**

| Route type | What it is |
|---|---|
| `shortest` | The shortest route a bike may legally use: no motorways or steps, and no footways unless they're signed for cycling |
| `low_stress_preferred` | As above, but each kind of way costs a multiple of its length, so the route uses paths and quiet streets where they exist. Main roads ×4, painted lanes ×1.6, tracks and untagged paths ×1.3, quiet streets ×1.15, shared paths ×1.05 |
| `low_stress_only` | Only cycleways, shared paths, cycle tracks and quiet streets. `found = false` means there's no such route |

## Classification

Public. Derived from open data; no personal data.

## Owner

Dan. See the gov-etl repo for the code.

## Source & references

- Network: `incoming.osm_paths` and `incoming.osm_points` (OpenStreetMap, 2026-11-11); climb from `incoming.osm_path_gradients`.
- Site: the SP14 allocation boundary as drawn on thatchamplan.co.uk/sp17/ (`osm_cycle_routes/sp14_boundary.geojson`).
- Destinations: Thatcham station (NaPTAN `9100THATCHM`), Kennet School (URN 110055, location from the PCT), the Thatcham Broadway bus stops (NaPTAN `030055890001`/`2`).
- What counts as low stress loosely follows DfT Local Transport Note 1/20 (Cycle Infrastructure Design): https://www.gov.uk/government/publications/cycle-infrastructure-design-ltn-120. The classification below is this table's own simplification, not LTN 1/20's assessment method.

## License & usage restrictions

Contains OpenStreetMap data (© OpenStreetMap contributors, ODbL 1.0; a derived database, so share it under the ODbL), Environment Agency lidar and NaPTAN (Open Government Licence v3.0).

## Location

- S3: `s3://dantelore.data.incoming/osm_cycle_routes/osm_cycle_routes.parquet`, `osm_cycle_route_segments/…`, `osm_cycle_network_ends/…`
- Glue: `incoming.osm_cycle_routes`, `incoming.osm_cycle_route_segments`, `incoming.osm_cycle_network_ends`
- Format: Parquet, not partitioned

## Field spec

**Provision classes**, used in all three tables:

| Class | Ways |
|---|---|
| `cycleway` | Cycleways (and paths signed for bikes) that are cycle-only or segregated from walkers |
| `shared_path` | Paths shared by walking and cycling: unsegregated cycleways, footways and paths with cycle permission, bridleways |
| `cycle_track` | Roads tagged with a separate cycle track alongside (`cycleway = track/separate`) |
| `quiet_road` | Residential streets, living streets, service roads and unclassified roads |
| `cycle_lane` | Main roads with painted cycle lanes |
| `main_road` | Tertiary, secondary, primary and trunk roads without cycle lanes |
| `track` | Farm and forest tracks |
| `path_unclear` | Paths with no cycling tag (often public footpaths, where cycling isn't allowed) |

Low stress means `cycleway`, `shared_path`, `cycle_track` and `quiet_road`.

### osm_cycle_routes

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| origin_id | bigint | No | Key | Origin point, 1 to 28 |
| origin_lon, origin_lat | double | No | | The point on the site (WGS84) |
| snap_m | double | No | | Distance from the point to where the route starts on the network |
| destination | string | No | Key | `Thatcham station`, `Kennet School`, `Town centre (Broadway)` |
| route_type | string | No | Key | `shortest`, `low_stress_preferred`, `low_stress_only` |
| found | boolean | No | | False if there is no route of this type |
| length_m | double | Yes | | Route length |
| climb_m | double | Yes | | Total climb from the site to the destination |
| descent_m | double | Yes | | Total descent on the way there, which is the climb on the way back along the same route |
| max_uphill_pct | double | Yes | | Steepest 50 m uphill on the way there, from ways of 30 m or more |
| low_stress_pct | double | Yes | | Share of the length on low-stress classes |
| cycleway_m, cycle_track_m, shared_path_m, quiet_road_m, track_m, path_unclear_m, cycle_lane_m, main_road_m | double | Yes | | Metres on each class |
| main_roads_crossed | bigint | Yes | | Times the route crosses (not rides along) a main road |
| unmarked_crossings | bigint | Yes | | Of those, crossings with no crossing mapped at the crossing point, or one mapped as unmarked or informal |
| unmarked_crossing_roads | string | Yes | | Which roads those are |
| longest_gap_m | double | Yes | | Longest continuous stretch not on low-stress provision |
| longest_gap_ways | string | Yes | | Streets (or way types) in that stretch |
| geometry_wkt | string | Yes | | The route, `LINESTRING(lon lat, …)` |

### osm_cycle_route_segments

The route keys (`origin_id`, `origin_lon`, `origin_lat`, `snap_m`, `destination`, `route_type`), then `seq` (order along the route), `osm_id`, `name` (or road number), `highway`, `provision`, `maxspeed`, `length_m`, `climb_m`, `descent_m` (in the direction ridden) and `geometry_wkt`.

### osm_cycle_network_ends

| Field name | Type | Nullable | Description |
|---|---|---|---|
| node_id | bigint | No | OSM node where the path ends |
| lon, lat | double | No | Location |
| ends_at | string | No | `main road` (a 2026-11-13 count of 64), `footway or way without cycle access` (45), `dead end` (20), `track or unclear path` (18) |
| path_osm_id, path_name, path_provision | various | | The path that ends |
| meets_name, meets_highway | string | Yes | The road or way it meets |
| crossing | string | Yes | Crossing mapped at that exact node, if any (`traffic_signals`, `uncontrolled`, `unmarked`, …) |

A path that carries on along another cycleway, shared path or quiet street isn't counted as ending.

## Common queries

Summary by destination and route type (medians across the site):

```sql
select destination, route_type,
       round(avg(case when found then 100.0 else 0 end)) as pct_of_site_with_route,
       round(approx_percentile(length_m, 0.5)) as metres,
       round(approx_percentile(climb_m, 0.5), 1) as climb_there,
       round(approx_percentile(descent_m, 0.5), 1) as climb_back,
       round(approx_percentile(low_stress_pct, 0.5)) as low_stress_pct,
       round(approx_percentile(longest_gap_m, 0.5)) as longest_gap_m
from incoming.osm_cycle_routes
group by 1, 2 order by 1, 2
```

On 2026-11-13 this gave:

- **Station:** about 2.5 km, climbing about 6 m there and **26 m back**.
- **Paths and quiet streets only:** there was such a route from 61% of the site to the station and to Kennet School, and from **none of it** to the town centre. Every route there runs along Chapel Street and The Broadway (a gap of about 770 m).

Where the paths stop at a main road:

```sql
select meets_name, count(*) as path_ends, count(crossing) as with_crossing_at_the_end
from incoming.osm_cycle_network_ends
where ends_at = 'main road'
group by 1 order by 2 desc
```

The streets in the gaps, for one destination:

```sql
select name, highway, provision, maxspeed, count(distinct origin_id) as routes, round(avg(length_m)) as metres
from incoming.osm_cycle_route_segments
where destination = 'Town centre (Broadway)' and route_type = 'low_stress_preferred'
  and provision not in ('cycleway', 'shared_path', 'cycle_track', 'quiet_road')
group by 1, 2, 3, 4 order by routes desc
```

## Source ETL / code

`github.com/dantelore/gov-etl`, `osm_cycle_routes/osm_cycle_routes_load.py`. Reads the Thatcham area (longitude −1.32 to −1.18, latitude 51.38 to 51.43) from the OSM tables and builds a routable network: one edge per pair of consecutive nodes on each way a bike may use, respecting one-way streets except where bikes are exempt. Then it runs Dijkstra from each origin to each destination for each route type. Takes a few seconds. Origins, destinations and costs are constants at the top of the script.

```bash
python osm_cycle_routes/osm_cycle_routes_load.py
```

## Freshness & update cadence

Manual. Rerun after reloading [osm](osm.md) and [osm_path_gradients](osm_path_gradients.md). Built 2026-11-13 from OSM data of 2026-11-11.

## Running costs (estimated)

From the S3 size on 2026-11-13. Method: [gov-etl README](README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | Under 0.001 GB, 3 objects | <$0.001 |
| **Total** | | **<$0.001** |

## Known issues & caveats

- **Only as good as OpenStreetMap's tagging.** A path missing its `bicycle = designated` tag counts as no cycling (footway) or unclear (path); a crossing mapped as a separate way rather than a node on the road may not be recognised; new paths may not be mapped yet. **Check any gap on the ground or on the map before publishing it.**
- **Not a level-of-service assessment.** Quiet streets are classed by road type, not traffic volume or speed; painted lanes on main roads are not counted as low stress; junction design, widths, surfaces and lighting aren't considered.
- **The site isn't built.** Routes start from the existing network nearest each point. The site's own streets, paths and access points (and any new crossings) will change the routes, especially for points far from today's network.
- **Crossings:** `unmarked_crossings` only looks for a crossing mapped at the exact node where the route meets the road. A crossing a few metres along can be missed, and `osm_cycle_network_ends.crossing` has the same limit. Treat them as places to check, not proof there's no crossing.
- Route climbs are summed from the 10 m profiles in [osm_path_gradients](osm_path_gradients.md), so they inherit its lidar caveats.
- One origin (origin 26, at −1.2220, 51.4035 in the south-east of the site) is joined to a fragment of track not connected to the rest of the network, so its routes are `found = false` for every type. The 61% for `low_stress_only` counts that origin as having no route.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|
| Thatcham analysis (thatchamplan.co.uk) | Dan | Testing whether there are continuous cycle routes from the site |

## Status

Active.
