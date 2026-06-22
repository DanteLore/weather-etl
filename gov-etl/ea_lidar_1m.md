# EA LiDAR 1m Composite DTM

## Description

1m-resolution bare-earth Digital Terrain Model (DTM) for England from the Environment
Agency's National LiDAR Programme. Voids are filled. Each row represents the SW corner of
one 1m grid cell. 60x finer resolution than OS Terrain 50 — useful for flood risk, drainage
analysis, urban morphology, and any application where 50m resolution is too coarse.

**Only selected areas are loaded** — the full England extent at 1m resolution would be
~390 GB. Data is fetched on demand by bounding box via the EA WCS endpoint and stored
under the standard 100km partition scheme. Currently loaded: West Berkshire.

## Classification

Public. Open government data; no personal data.

## Owner

Dan. See gov-etl repo for the code.

## Source & references

Environment Agency — LiDAR Composite DTM 1m, accessed via WCS.
https://environment.data.gov.uk/dataset/13787b9a-26a4-4775-8523-806d13af58fc

WCS base URL: `https://environment.data.gov.uk/geoservices/datasets/13787b9a-26a4-4775-8523-806d13af58fc/wcs`

## License & usage restrictions

Open Government Licence v3.0. Free to use with attribution to the Environment Agency.

## Location

- S3: `s3://dantelore.data.incoming/ea_lidar_1m/dtm/grid_e=<e>/grid_n=<n>/`
- Glue: `incoming.ea_lidar_1m_dtm`
- Format: Parquet, partitioned by `grid_e` and `grid_n` (100km OSGB tile indices)

Always filter on both `grid_e` and `grid_n` to get partition pruning.

## Currently loaded areas

| Area | Bounding box (OSGB36) | grid_e | grid_n |
|---|---|---|---|
| West Berkshire | `430000,155000,490000,190000` | 4 | 1 |

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| x_coordinate | int | No | Joins to os_open_uprn_uprn on grid_e+grid_n+exact cell | OSGB36 easting of cell SW corner (metres, EPSG:27700) |
| y_coordinate | int | No | | OSGB36 northing of cell SW corner (metres, EPSG:27700) |
| elevation_m | float | No | | Elevation above sea level (metres, bare earth) |
| grid_e | int | No | Partition key | `floor(x_coordinate / 100000)` — 100km easting tile (0–6) |
| grid_n | int | No | Partition key | `floor(y_coordinate / 100000)` — 100km northing tile (0–12) |

For exact-cell joins to UPRN: `e.x_coordinate = CAST(u.x_coordinate AS INT)`.

## Common queries

### Elevation at a specific OSGB coordinate (exact 1m cell)

```sql
SELECT x_coordinate, y_coordinate, elevation_m
FROM ea_lidar_1m_dtm
WHERE grid_e = 4 AND grid_n = 1
  AND x_coordinate = 453000
  AND y_coordinate = 167000
```

### Elevation range across a neighbourhood

```sql
SELECT
    MIN(elevation_m) AS min_elev,
    MAX(elevation_m) AS max_elev,
    AVG(elevation_m) AS mean_elev
FROM ea_lidar_1m_dtm
WHERE grid_e = 4 AND grid_n = 1
  AND x_coordinate BETWEEN 452000 AND 454000
  AND y_coordinate BETWEEN 166000 AND 168000
```

### Join UPRN addresses to their precise 1m elevation

```sql
SELECT
    u.uprn,
    u.x_coordinate AS uprn_e,
    u.y_coordinate AS uprn_n,
    e.elevation_m
FROM os_open_uprn_uprn u
JOIN ea_lidar_1m_dtm e
    ON  e.grid_e = u.grid_e
    AND e.grid_n = u.grid_n
    AND e.x_coordinate = CAST(u.x_coordinate AS INT)
    AND e.y_coordinate = CAST(u.y_coordinate AS INT)
WHERE u.grid_e = 4 AND u.grid_n = 1
```

### Flood risk proxy — addresses below a threshold elevation

```sql
SELECT u.uprn, u.x_coordinate, u.y_coordinate, e.elevation_m
FROM os_open_uprn_uprn u
JOIN ea_lidar_1m_dtm e
    ON  e.grid_e = u.grid_e
    AND e.grid_n = u.grid_n
    AND e.x_coordinate = CAST(u.x_coordinate AS INT)
    AND e.y_coordinate = CAST(u.y_coordinate AS INT)
WHERE u.grid_e = 4 AND u.grid_n = 1
  AND e.elevation_m < 50
ORDER BY e.elevation_m
```

### Compare LiDAR to OS Terrain 50 for the same area

```sql
SELECT
    l.x_coordinate,
    l.y_coordinate,
    l.elevation_m AS lidar_1m,
    t.elevation_m AS terrain_50m
FROM ea_lidar_1m_dtm l
JOIN os_terrain_50_terrain50 t
    ON  t.grid_e = l.grid_e
    AND t.grid_n = l.grid_n
    AND t.x_coordinate = (CAST(l.x_coordinate / 50 AS INT) * 50)
    AND t.y_coordinate = (CAST(l.y_coordinate / 50 AS INT) * 50)
WHERE l.grid_e = 4 AND l.grid_n = 1
  AND l.x_coordinate BETWEEN 453000 AND 453100
  AND l.y_coordinate BETWEEN 167000 AND 167100
```

## Loading additional areas

```bash
# Add a new area by bounding box (min_e, min_n, max_e, max_n in OSGB36 metres)
python ea_lidar_1m/ea_lidar_1m_load.py --bbox 450000,170000,560000,220000

# Add a single 10km test chunk
python ea_lidar_1m/ea_lidar_1m_load.py --bbox 530000,180000,540000,190000
```

Chunks already uploaded are not re-uploaded. Chunks outside EA coverage are silently
skipped (the WCS returns an exception report which the loader treats as no data).

## Source ETL / code

`github.com/dantelore/gov-etl`, `ea_lidar_1m/ea_lidar_1m_load.py`.
Fetches 10km bounding-box chunks via WCS (GeoTIFF), converts to Parquet using rasterio.

## Freshness & update cadence

The EA update the composite periodically as new survey flights are completed. Re-run the
loader with the same bbox to refresh a previously loaded area.

## Known issues & caveats

- England only — Scotland and Wales are not covered by this dataset. Use `os_terrain_50`
  for GB-wide coverage.
- Only selected areas are loaded. Querying an unloaded area returns no rows rather than
  an error — check the loaded areas table above before assuming data is missing.
- Each 10km chunk contains up to 100M points — queries without tight coordinate filters
  within a partition can be slow and costly.
- Some rural or recently surveyed areas may have older or lower-quality source flights
  within the composite.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active (partial coverage — West Berkshire loaded).
