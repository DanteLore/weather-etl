# OS Open UPRN

## Description

OSGB36 and WGS84 coordinates for every Unique Property Reference Number (UPRN) in Great
Britain — roughly 42 million address points covering residential, commercial and other
properties. A UPRN is the persistent identifier for an addressable location in the UK.
Useful for spatially locating individual addresses, joining to postcode-level datasets,
and counting addresses within an area.

## Classification

Public. Open government data; no personal data beyond address coordinates.

## Owner

Dan. See gov-etl repo for the code.

## Source & references

Ordnance Survey — OS Open UPRN, downloaded via OS Data Hub API (no key required).
https://osdatahub.os.uk/downloads/open/OpenUPRN

## License & usage restrictions

Open Government Licence v3.0. Free to use with attribution to OS.

## Location

- S3: `s3://dantelore.data.incoming/os_open_uprn/uprn/grid_e=<e>/grid_n=<n>/`
- Glue: `incoming.os_open_uprn_uprn`
- Format: Parquet, partitioned by `grid_e` and `grid_n` (100km OSGB tile indices)

Always filter on both `grid_e` and `grid_n` to get partition pruning. See the Conventions
section in the gov-etl README for the spatial partitioning standard.

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| uprn | bigint | No | Primary | Unique Property Reference Number |
| x_coordinate | double | Yes | | OSGB36 easting (metres, EPSG:27700) |
| y_coordinate | double | Yes | | OSGB36 northing (metres, EPSG:27700) |
| latitude | double | Yes | | WGS84 latitude (EPSG:4326) |
| longitude | double | Yes | | WGS84 longitude (EPSG:4326) |
| grid_e | int | No | Partition key | `floor(x_coordinate / 100000)` — 100km easting tile (0–6) |
| grid_n | int | No | Partition key | `floor(y_coordinate / 100000)` — 100km northing tile (0–12) |

## Source ETL / code

`github.com/dantelore/gov-etl`, `os_open_uprn/os_open_uprn_load.py`.

## Freshness & update cadence

OS publish Open UPRN updates quarterly. Re-run the loader to refresh — it downloads the
full dataset and re-partitions from scratch.

## Known issues & caveats

- A small number of UPRNs have null coordinates (non-geographic or pending addresses).
- `x_coordinate`/`y_coordinate` are stored as `double` (not `int`) in this table, unlike
  the elevation datasets which use `int`. Cast as needed when joining on coordinates.
- Coverage is Great Britain only — no Northern Ireland UPRNs.
- ~42M rows; always use partition filters — an unfiltered scan is expensive.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
