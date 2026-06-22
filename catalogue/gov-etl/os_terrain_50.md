# OS Terrain 50

## Description

50m-resolution Digital Terrain Model (DTM) covering all of Great Britain, from Ordnance
Survey's Terrain 50 product. Each row represents the SW corner of one 50m grid cell with
its elevation above sea level. ~114 million points covering the full GB land area. Useful
as a lightweight elevation dataset when 1m LiDAR precision is not required, and as the
only elevation source for Scotland and Wales in this data lake.

## Classification

Public. Open government data; no personal data.

## Owner

Dan. See gov-etl repo for the code.

## Source & references

Ordnance Survey — Terrain 50, downloaded via OS Data Hub API (no key required).
https://osdatahub.os.uk/downloads/open/Terrain50

## License & usage restrictions

Open Government Licence v3.0. Free to use with attribution to OS.

## Location

- S3: `s3://dantelore.data.incoming/os_terrain_50/terrain50/grid_e=<e>/grid_n=<n>/`
- Glue: `incoming.os_terrain_50_terrain50`
- Format: Parquet, partitioned by `grid_e` and `grid_n` (100km OSGB tile indices)

Always filter on both `grid_e` and `grid_n` to get partition pruning.

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| x_coordinate | int | No | Joins to os_open_uprn_uprn on grid_e+grid_n+nearest cell | OSGB36 easting of cell SW corner (metres, EPSG:27700) |
| y_coordinate | int | No | | OSGB36 northing of cell SW corner (metres, EPSG:27700) |
| elevation_m | float | No | | Elevation above sea level (metres) |
| grid_e | int | No | Partition key | `floor(x_coordinate / 100000)` — 100km easting tile (0–6) |
| grid_n | int | No | Partition key | `floor(y_coordinate / 100000)` — 100km northing tile (0–12) |

For nearest-cell joins, snap coordinates to the 50m grid: `CAST(x / 50 AS INT) * 50`.

## Common queries

### Elevation at a specific OSGB coordinate (nearest 50m cell)

```sql
SELECT x_coordinate, y_coordinate, elevation_m
FROM os_terrain_50_terrain50
WHERE grid_e = 5 AND grid_n = 1
  AND x_coordinate = (CAST(530123 / 50 AS INT) * 50)
  AND y_coordinate = (CAST(180456 / 50 AS INT) * 50)
```

### Elevation statistics across a 10km tile

```sql
SELECT
    AVG(elevation_m) AS mean_elevation,
    MIN(elevation_m) AS min_elevation,
    MAX(elevation_m) AS max_elevation
FROM os_terrain_50_terrain50
WHERE grid_e = 4 AND grid_n = 1
  AND x_coordinate BETWEEN 430000 AND 440000
  AND y_coordinate BETWEEN 155000 AND 165000
```

### Join UPRN addresses to their nearest elevation

```sql
SELECT
    u.uprn,
    u.x_coordinate AS uprn_e,
    u.y_coordinate AS uprn_n,
    t.elevation_m
FROM os_open_uprn_uprn u
JOIN os_terrain_50_terrain50 t
    ON  t.grid_e = u.grid_e
    AND t.grid_n = u.grid_n
    AND t.x_coordinate = (CAST(u.x_coordinate / 50 AS INT) * 50)
    AND t.y_coordinate = (CAST(u.y_coordinate / 50 AS INT) * 50)
WHERE u.grid_e = 4 AND u.grid_n = 1
```

### Elevation profile across West Berkshire

```sql
SELECT x_coordinate, y_coordinate, elevation_m
FROM os_terrain_50_terrain50
WHERE grid_e IN (4, 5) AND grid_n = 1
  AND x_coordinate BETWEEN 430000 AND 490000
  AND y_coordinate BETWEEN 155000 AND 190000
ORDER BY y_coordinate DESC, x_coordinate
```

### Highest points in a 100km square

```sql
SELECT x_coordinate, y_coordinate, elevation_m
FROM os_terrain_50_terrain50
WHERE grid_e = 4 AND grid_n = 1
ORDER BY elevation_m DESC
LIMIT 10
```

## Source ETL / code

`github.com/dantelore/gov-etl`, `os_terrain_50/os_terrain_50_load.py`.
Downloads a ZIP-of-ZIPs (~2,858 inner ZIPs, one per 10km tile), parses ASC grid files,
and uploads partitioned Parquet. Caches the outer ZIP in `%TEMP%\gov_etl_cache\` between
runs so a restart doesn't re-download the full dataset.

## Freshness & update cadence

OS publish Terrain 50 updates periodically (roughly annually). Re-run the loader to refresh.
Delete the cached zip first if you want to force a re-download:
`del %TEMP%\gov_etl_cache\os_terrain_50.zip`

## Known issues & caveats

- 50m resolution — adequate for regional terrain analysis but too coarse for urban flood
  risk, line-of-sight, or site-level analysis. Use `ea_lidar_1m` for England if higher
  resolution is needed.
- Void cells (sea, outside OS coverage) are excluded from the Parquet files at load time.
- Elevation range in this dataset: -134.9m (Fens) to 1,345.1m (Ben Nevis area) — sanity
  check queries against these bounds if something looks wrong.
- West Berkshire elevation range: 20–297m, mean 103m.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
