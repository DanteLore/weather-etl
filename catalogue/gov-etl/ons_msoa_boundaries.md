# ONS MSOA Boundaries 2021

## Description

Polygon boundaries for all 7,264 Middle Layer Super Output Areas (MSOAs) in England and
Wales, 2021 Census vintage. MSOAs are the ONS standard small-area geography with roughly
7,500 residents each — fine enough for neighbourhood-level analysis, stable enough to
compare across datasets. Includes Rural-Urban Classification (RUC) for each area. Boundaries
use Super Generalised Clipped geometry.

## Classification

Public. Open government data; no personal data.

## Owner

Dan. See gov-etl repo for the code.

## Source & references

ONS Open Geography Portal — `MSOA_2021_EW_BSC_V3_RUC` ArcGIS FeatureServer.

## License & usage restrictions

Open Government Licence v3.0. Free to use with attribution to ONS.

## Location

- S3: `s3://dantelore.data.incoming/ons_msoa_boundaries/msoa/msoa_2021.parquet`
- Glue: `incoming.ons_msoa_boundaries_msoa`
- Format: Single Parquet file, no partitioning (7,264 rows)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| msoa21cd | string | No | Primary; joins to ons_msoa_income_income and ons_postcode_lookup_lookup | MSOA 2021 code (e.g. `E02003552`) |
| msoa21nm | string | No | | MSOA name in English (e.g. `Newbury 001`) |
| msoa21nmw | string | Yes | | MSOA name in Welsh (Wales only, null elsewhere) |
| ruc21cd | string | No | | Rural-Urban Classification code (e.g. `D1`, `C2`) |
| ruc21nm | string | No | | Rural-Urban Classification description (e.g. `Rural village`) |
| geometry_wgs84_wkt | string | No | | Polygon in WGS84 (EPSG:4326) as WKT |
| geometry_osgb_wkt | string | No | | Polygon in British National Grid (EPSG:27700) as WKT |
| centre_e | int | No | | Centroid easting (OSGB metres) |
| centre_n | int | No | | Centroid northing (OSGB metres) |
| bbox_min_e | int | No | | Bounding rectangle min easting (OSGB metres) |
| bbox_min_n | int | No | | Bounding rectangle min northing (OSGB metres) |
| bbox_max_e | int | No | | Bounding rectangle max easting (OSGB metres) |
| bbox_max_n | int | No | | Bounding rectangle max northing (OSGB metres) |

## Rural-Urban Classification codes

| Code | Description |
|---|---|
| `A1` | Urban major conurbation |
| `B1` | Urban minor conurbation |
| `C1` | Urban city and town |
| `C2` | Urban city and town in a sparse setting |
| `D1` | Rural town and fringe |
| `D2` | Rural town and fringe in a sparse setting |
| `E1` | Rural village and dispersed |
| `E2` | Rural village and dispersed in a sparse setting |

## Common queries

### Look up MSOAs by name

```sql
SELECT msoa21cd, msoa21nm, ruc21nm, centre_e, centre_n
FROM ons_msoa_boundaries_msoa
WHERE msoa21nm LIKE 'Newbury%'
ORDER BY msoa21nm
```

### Find MSOAs within a bounding box

```sql
-- MSOAs whose centroid falls within ~10km of Newbury town centre (448000, 167000)
SELECT msoa21cd, msoa21nm, ruc21nm
FROM ons_msoa_boundaries_msoa
WHERE centre_e BETWEEN 438000 AND 458000
  AND centre_n BETWEEN 157000 AND 177000
ORDER BY msoa21nm
```

### All rural MSOAs

```sql
SELECT msoa21cd, msoa21nm, ruc21cd, ruc21nm
FROM ons_msoa_boundaries_msoa
WHERE ruc21cd LIKE 'D%' OR ruc21cd LIKE 'E%'
ORDER BY msoa21nm
```

### Join to income estimates

```sql
SELECT
    b.msoa21nm,
    b.ruc21nm,
    i.net_annual_income,
    i.net_income_after_housing
FROM ons_msoa_boundaries_msoa b
JOIN ons_msoa_income_income i ON b.msoa21cd = i.msoa21cd
WHERE b.centre_e BETWEEN 430000 AND 470000
  AND b.centre_n BETWEEN 150000 AND 190000
ORDER BY i.net_annual_income DESC
```

## Working with WKT geometry

Athena has no native spatial functions. Use bbox columns for a fast coarse filter in Athena,
then apply exact geometry tests in Python with Shapely:

```python
from shapely import wkt
from shapely.geometry import Point

polygon = wkt.loads(row["geometry_osgb_wkt"])
point = Point(easting, northing)
if polygon.contains(point):
    ...
```

## Source ETL / code

`github.com/dantelore/gov-etl`, `ons_msoa_boundaries/ons_msoa_boundaries_load.py`.

## Freshness & update cadence

MSOA boundaries are updated after each Census. Current vintage is 2021; next revision
expected ~2031.

## Known issues & caveats

- Super Generalised Clipped geometry — not for precise boundary measurements.
- England and Wales only; no Scotland or Northern Ireland MSOAs.
- WKT geometry strings can be large — avoid `SELECT *` over many rows.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
