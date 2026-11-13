# OpenStreetMap Path Gradients (from EA 1m Lidar)

## Description

How steep every road and path is, wherever the EA 1m lidar is loaded: one row per OpenStreetMap way with its start, end, lowest and highest heights, the climb and descent along it, the steepest 50 m uphill and downhill, and a height profile every 10 m. 145,500 ways: 48,934 around Newbury and Thatcham (the West Berkshire lidar tiles) and 96,566 in the one London tile loaded. Built from [osm](osm.md) (`incoming.osm_paths`) and [ea_lidar_1m](ea_lidar_1m.md). Used to measure the climb on cycle routes, e.g. from Thatcham station (in the Kennet valley) up to the north-east of the town.

## Classification

Public. Derived from open data; no personal data.

## Owner

Dan. See the gov-etl repo for the code.

## Source & references

Derived from `incoming.osm_paths` (OpenStreetMap, 2026-11-11) and `incoming.ea_lidar_1m_dtm` (Environment Agency National LiDAR Programme composite DTM).

## License & usage restrictions

Contains OpenStreetMap data (© OpenStreetMap contributors, ODbL 1.0; the table is a derived database, so share it under the ODbL) and Environment Agency data (Open Government Licence v3.0).

## Location

- S3: `s3://dantelore.data.incoming/osm_path_gradients/grid_e=<e>/grid_n=<n>/osm_path_gradients_<e>_<n>.parquet`
- Glue: `incoming.osm_path_gradients`
- Format: Parquet, partitioned by `grid_e` and `grid_n` as `osm_paths` (the 100 km square of the way's first node)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| osm_id | bigint | No | Primary; joins to osm_paths | OSM way ID |
| highway, name | string | Yes | | As in `osm_paths` |
| length_m | double | No | | Way length |
| elevation_start_m, elevation_end_m | double | No | | Height at the first and last node (metres above sea level) |
| elevation_min_m, elevation_max_m | double | No | | Lowest and highest point along the way |
| climb_m | double | No | | Total climb going in the direction the way is drawn (first node to last) |
| descent_m | double | No | | Total descent in that direction, which is the climb going the other way |
| max_uphill_pct | double | No | | Steepest uphill over any 50 m in the drawn direction (or over the whole way if shorter) |
| max_downhill_pct | double | No | | Steepest downhill in the drawn direction: the steepest uphill going the other way |
| mean_gradient_pct | double | No | | (end height − start height) ÷ length |
| is_interpolated | boolean | No | | True for ways tagged `bridge` or `tunnel`: heights are a straight line between the ends (6,857 ways) |
| gradient_check | string | Yes | | Why the figures may be wrong, if they might be: `below sea level`, `road steeper than 20%`, `steeper than 40%`, `jump of over 5 m in 10 m` (on roads), comma-separated. Null for most ways. On 2026-11-13: 58 ways flagged in West Berkshire, 812 in the London tile |
| elevation_profile_m | array<double> | No | | Heights every 10 m from the first node, then at the last node |
| grid_e, grid_n | int | No | Partition keys | As `osm_paths` |

## Common queries

Steepest streets in Thatcham (ways of 100 m or more):

```sql
select g.name, g.highway, round(g.length_m) as metres, g.climb_m, g.descent_m,
       greatest(g.max_uphill_pct, g.max_downhill_pct) as steepest_pct,
       g.elevation_min_m, g.elevation_max_m
from incoming.osm_path_gradients g
join incoming.osm_paths p on p.osm_id = g.osm_id and p.grid_e = 4 and p.grid_n = 1
where g.grid_e = 4 and g.grid_n = 1 and g.length_m >= 100 and g.gradient_check is null
  and p.min_lon > -1.30 and p.max_lon < -1.20 and p.min_lat > 51.39 and p.max_lat < 51.42
order by steepest_pct desc limit 20
```

Height range of named streets (e.g. how far Harts Hill Road climbs):

```sql
select name, round(sum(length_m)) as metres, min(elevation_min_m) as lowest, max(elevation_max_m) as highest
from incoming.osm_path_gradients
where grid_e = 4 and grid_n = 1 and name in ('Harts Hill Road', 'Floral Way', 'Bath Road', 'Station Road')
group by 1
```

A route's climb: add `climb_m` for ways ridden in their drawn direction and `descent_m` for ways ridden backwards.

## Source ETL / code

`github.com/dantelore/gov-etl`, `osm_gradients/osm_gradients_load.py`. For each grid square that has lidar, it reads each 10 km lidar tile into a 1 m height grid (about 400 MB each), and for each way that touches one it:

1. samples a height every 10 m (the median of the 3 × 3 m around the point);
2. smooths the profile with a 90 m running median. This leaves any steady slope exactly as it is but removes dips and spikes up to about 40 m wide: unmapped bridges and culverts, kerbs and banks, and the few metres OSM lines can be off;
3. checks the last few samples at each end for an abrupt step that breaks from the trend (typically a way ending at a bridge abutment, where the lidar drops to the road below) and continues the trend instead;
4. interpolates between the ends for bridges and tunnels;
5. flags anything still implausible in `gradient_check`.

Ways with more than 20% of samples outside the lidar are skipped. Takes about 3 minutes.

```bash
python osm_gradients/osm_gradients_load.py
```

## Freshness & update cadence

Manual. Rerun after reloading `osm_paths` or loading more lidar (each square's file is replaced). Built 2026-11-13 from OSM data of 2026-11-11 (rebuilt the same day with the wider smoothing, end check and `gradient_check`).

## Running costs (estimated)

From the S3 size on 2026-11-13. Method: [gov-etl README](README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.007 GB, 2 objects | <$0.001 |
| **Total** | | **<$0.001** |

## Known issues & caveats

- **Only where the lidar is loaded**: ten 10 km tiles in West Berkshire and one in London (see [ea_lidar_1m](ea_lidar_1m.md)). The West Berkshire tiles cover Newbury, Thatcham and the land south to Crookham Common, but not Theale or the area east of Newbury north of about 175000 N. Use [os_terrain_50](os_terrain_50.md) for rough figures elsewhere.
- **The lidar is bare earth.** Bridges are removed, so ways on bridges would dip to what's below. Ways tagged `bridge` are interpolated, and short unmapped bridges are smoothed out. Longer unmapped structures, ways drawn on a cutting or embankment slope, and ground dug out or built up since the survey can't be fixed from the lidar, and show up in `gradient_check` instead. Filter `gradient_check is null` for figures you'd quote.
- **Very steep isn't always wrong.** Ways of 50 m or more, steepest 50 m either way:

  | Area | 99th percentile | Steepest unflagged | Steepest overall |
  |---|---|---|---|
  | West Berkshire | 17% | 39%: paths on the North Wessex Downs escarpment around Inkpen and Walbury Hill, which really are that steep | 53%: a footway drawn straight down that scarp, flagged |
  | London tile | 8% | | 24% |
- Short ways (under about 50 m) give noisy maxima, because a single 10 m step decides them.
- Heights are of the ground at the way's line, not the road surface; steps and ramps are as steep as the ground they're drawn on.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|
| Thatcham analysis (thatchamplan.co.uk) | Dan | Hills on cycle routes |

## Status

Active.
