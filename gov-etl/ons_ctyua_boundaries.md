# ONS County and Unitary Authority Boundaries

## Description

Polygon boundaries for 205 Counties and Unitary Authorities (CTYUAs) covering England,
Wales and Scotland, April 2019 vintage (Ultra Generalised Clipped Boundaries). CTYUAs are
upper-tier administrative areas sitting above districts and wards. Useful for grouping data
at a regional/county level, and for spatial filtering when BUA boundaries are too granular.
Includes West Berkshire (`E06000037`), Cornwall, Highland, and all Welsh authorities.

## Classification

Public. Open government data; no personal data.

## Owner

Dan. See gov-etl repo for the code.

## Source & references

ONS Open Geography Portal — `CTYUA_Apr_2019_UGCB_Great_Britain_2022` ArcGIS FeatureServer
(Ultra Generalised Clipped Boundaries variant).

## License & usage restrictions

Open Government Licence v3.0. Free to use with attribution to ONS.

## Location

- S3: `s3://dantelore.data.incoming/ons_ctyua_boundaries/ctyua/ctyua_2025.parquet`
- Glue: `incoming.ons_ctyua_boundaries_ctyua`
- Format: Single Parquet file, no partitioning (205 rows)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| ctyua25cd | string | No | Primary | Stable ONS area code (e.g. `E06000037`) |
| ctyua25nm | string | No | | Area name in English (e.g. `West Berkshire`) |
| ctyua25nmw | string | Yes | | Area name in Welsh (Wales only, null elsewhere) |
| geometry_wgs84_wkt | string | No | | Polygon/MultiPolygon in WGS84 (EPSG:4326) as WKT |
| geometry_osgb_wkt | string | No | | Polygon/MultiPolygon in British National Grid (EPSG:27700) as WKT |
| centre_e | int | No | | Centroid easting (OSGB metres) |
| centre_n | int | No | | Centroid northing (OSGB metres) |
| bbox_min_e | int | No | | Bounding rectangle min easting (OSGB metres) |
| bbox_min_n | int | No | | Bounding rectangle min northing (OSGB metres) |
| bbox_max_e | int | No | | Bounding rectangle max easting (OSGB metres) |
| bbox_max_n | int | No | | Bounding rectangle max northing (OSGB metres) |

## Common queries

### Look up a single area by name

```sql
SELECT ctyua25cd, ctyua25nm, centre_e, centre_n
FROM ons_ctyua_boundaries_ctyua
WHERE ctyua25nm = 'West Berkshire'
```

### List all areas

```sql
SELECT ctyua25cd, ctyua25nm, centre_e, centre_n
FROM ons_ctyua_boundaries_ctyua
ORDER BY ctyua25nm
```

### Find areas by approximate location

```sql
-- CTYUAs whose centroid falls within ~50km of central London (531000, 181000)
SELECT ctyua25cd, ctyua25nm, centre_e, centre_n
FROM ons_ctyua_boundaries_ctyua
WHERE centre_e BETWEEN 481000 AND 581000
  AND centre_n BETWEEN 131000 AND 231000
```

### Aggregate house prices by CTYUA

Note: `county` in house price data doesn't always match a CTYUA name. For accurate spatial
joining, use the postcode → CTYUA lookup via the ONS postcode lookup table.

```sql
SELECT
    c.ctyua25nm,
    COUNT(*) AS sales,
    ROUND(AVG(p.price)) AS avg_price
FROM house_prices_ppd p
JOIN ons_ctyua_boundaries_ctyua c ON p.county = c.ctyua25nm
WHERE p.year = '2023'
GROUP BY c.ctyua25nm
ORDER BY avg_price DESC
LIMIT 20
```

### Area sizes (bounding rectangle)

```sql
SELECT
    ctyua25nm,
    (bbox_max_e - bbox_min_e)                                        AS width_m,
    (bbox_max_n - bbox_min_n)                                        AS height_m,
    (bbox_max_e - bbox_min_e) * (bbox_max_n - bbox_min_n) / 1000000 AS bbox_area_km2
FROM ons_ctyua_boundaries_ctyua
ORDER BY bbox_area_km2 DESC
LIMIT 20
```

## Working with WKT geometry

Athena has no native spatial functions. Use bbox columns for fast rectangular pre-filtering
in Athena, then apply exact geometry tests in Python with Shapely:

```python
from shapely import wkt
from shapely.geometry import Point

polygon = wkt.loads(row["geometry_osgb_wkt"])
point = Point(easting, northing)
if polygon.contains(point):
    ...
```

Use `geometry_wgs84_wkt` for tools expecting lat/lng (Leaflet, Google Maps);
use `geometry_osgb_wkt` for distance calculations and joins against OSGB coordinates.

Note: full-resolution `_NC` boundary services exceed the ArcGIS per-feature transfer limit
and cannot be paged with geometry — the Ultra Generalised Clipped variant is used here.
If a newer UGCB vintage is published, update `ARCGIS_URL` in the loader and re-run.

## Source ETL / code

`github.com/dantelore/gov-etl`, `ons_ctyua_boundaries/ons_ctyua_boundaries_load.py`.

## Freshness & update cadence

Boundaries are updated infrequently. Current vintage is April 2019 (codes remain current).
Check the ONS Open Geography Portal for new UGCB releases and update `ARCGIS_URL` in the
loader when available.

## Known issues & caveats

- Ultra Generalised Clipped (~20m tolerance) — not suitable for precise boundary measurements.
  Full-resolution variants exceed the ArcGIS per-feature transfer limit and cannot be loaded.
- `ctyua22cd` codes used in the postcode lookup table are a 2022 vintage; the boundaries
  here use 2019-vintage codes (`ctyua25cd`). For most authorities they match, but verify
  before joining by code.
- Coverage is Great Britain only — no Northern Ireland.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
