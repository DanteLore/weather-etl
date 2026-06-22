# ONS Built-up Area Boundaries 2024

## Description

Polygon boundaries for all 7,775 Built-up Areas (BUAs) in England and Wales, April 2024
vintage. A BUA is a contiguous area of urban development — towns, cities and suburbs —
defined by the ONS from OS MasterMap Topography data. Useful for spatial filtering,
grouping point data by urban area, and display. Includes centroid and bounding-rectangle
columns for fast rectangular pre-filtering without parsing WKT geometry.

## Classification

Public. Open government data; no personal data.

## Owner

Dan. See gov-etl repo for the code.

## Source & references

ONS Open Geography Portal — `main_ONS_BUA_2024_EW_V2` ArcGIS FeatureServer.

## License & usage restrictions

Open Government Licence v3.0. Free to use with attribution to ONS.

## Location

- S3: `s3://dantelore.data.incoming/ons_bua_boundaries/bua/bua_2024.parquet`
- Glue: `incoming.ons_bua_boundaries_bua`
- Format: Single Parquet file, no partitioning (7,775 rows)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| bua24cd | string | No | Primary | Stable ONS area code (e.g. `E63012319`) |
| bua24nm | string | No | | Area name (e.g. `Newbury`) |
| geometry_wgs84_wkt | string | No | | Polygon/MultiPolygon in WGS84 (EPSG:4326, lng/lat) as WKT |
| geometry_osgb_wkt | string | No | | Polygon/MultiPolygon in British National Grid (EPSG:27700, metres) as WKT |
| centre_e | int | No | | Centroid easting (OSGB metres) |
| centre_n | int | No | | Centroid northing (OSGB metres) |
| bbox_min_e | int | No | | Bounding rectangle min easting (OSGB metres) |
| bbox_min_n | int | No | | Bounding rectangle min northing (OSGB metres) |
| bbox_max_e | int | No | | Bounding rectangle max easting (OSGB metres) |
| bbox_max_n | int | No | | Bounding rectangle max northing (OSGB metres) |

Athena has no native spatial functions. Use `centre_e`/`centre_n` or bbox columns for
rectangular pre-filtering in SQL, then apply exact point-in-polygon tests in Python with
Shapely using `geometry_osgb_wkt`.

## Common queries

### Look up a single area by name

```sql
SELECT bua24cd, bua24nm, centre_e, centre_n
FROM ons_bua_boundaries_bua
WHERE bua24nm = 'Newbury'
```

### Find areas near a point (centroid filter)

```sql
-- Areas with centroid within ~20km of Newbury town centre (448000, 167000)
SELECT bua24cd, bua24nm, centre_e, centre_n
FROM ons_bua_boundaries_bua
WHERE centre_e BETWEEN 428000 AND 468000
  AND centre_n BETWEEN 147000 AND 187000
```

### Find areas overlapping a bounding box

```sql
-- BUAs whose bounding rectangle overlaps a query rectangle
SELECT bua24cd, bua24nm
FROM ons_bua_boundaries_bua
WHERE bbox_max_e >= 440000
  AND bbox_min_e <= 460000
  AND bbox_max_n >= 160000
  AND bbox_min_n <= 175000
```

### Join to house price data by area name

```sql
SELECT
    b.bua24nm,
    COUNT(*) AS sales,
    ROUND(AVG(p.price)) AS avg_price
FROM house_prices_ppd p
JOIN ons_bua_boundaries_bua b ON p.town_city = b.bua24nm
WHERE p.year = '2023'
GROUP BY b.bua24nm
ORDER BY avg_price DESC
```

Note: `town_city` in the house price data is free text and won't always match `bua24nm`
exactly — consider normalising case or using a postcode-to-BUA lookup via OS Code Point Open.

### List areas sorted by bounding rectangle size

```sql
SELECT
    bua24nm,
    (bbox_max_e - bbox_min_e)                                        AS width_m,
    (bbox_max_n - bbox_min_n)                                        AS height_m,
    (bbox_max_e - bbox_min_e) * (bbox_max_n - bbox_min_n) / 1000000 AS bbox_area_km2
FROM ons_bua_boundaries_bua
ORDER BY bbox_area_km2 DESC
LIMIT 20
```

## Working with WKT geometry

Athena has no native spatial functions. The WKT columns are most useful when exporting to a
GIS tool (QGIS, GeoPandas) for exact point-in-polygon tests, or passing geometries to another
system that accepts WKT (PostGIS, Spark). For point-in-polygon in Python after a coarse Athena
bbox pre-filter:

```python
from shapely import wkt
from shapely.geometry import Point

polygon = wkt.loads(row["geometry_osgb_wkt"])
point = Point(easting, northing)
if polygon.contains(point):
    ...
```

Use `geometry_wgs84_wkt` for tools expecting lat/lng (Leaflet, Google Maps);
use `geometry_osgb_wkt` for distance calculations and joins against OSGB datasets.

## Source ETL / code

`github.com/dantelore/gov-etl`, `ons_bua_boundaries/ons_bua_boundaries_load.py`.

## Freshness & update cadence

BUA boundaries are updated infrequently — current vintage is April 2024. Check the ONS
Open Geography Portal for new releases and re-run the loader when a new vintage is published.

## Known issues & caveats

- WKT geometry strings can be large (tens to hundreds of KB per feature) — avoid `SELECT *`
  over many rows.
- Coverage is England and Wales only — no Scotland or Northern Ireland.
- `bua24nm` is not always a reliable join key to free-text fields like `town_city` in house
  price data due to case and spelling differences.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
