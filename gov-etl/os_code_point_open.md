# OS Code Point Open

## Description

OSGB36 easting/northing coordinates for every live postcode in Great Britain (~1.8 million
postcodes), plus administrative area codes. The standard dataset for geocoding postcodes to
OSGB coordinates. Coordinates represent the mean position of all addresses within the
postcode unit. Useful for plotting postcodes on a map, computing distances, and spatial
joins against OSGB-coordinate datasets (UPRN, traffic census, elevation).

## Classification

Public. Open government data; no personal data.

## Owner

Dan. See gov-etl repo for the code.

## Source & references

Ordnance Survey — Code Point Open, downloaded via OS Data Hub API (no key required).
https://osdatahub.os.uk/downloads/open/CodePointOpen

## License & usage restrictions

Open Government Licence v3.0. Free to use with attribution to OS.

## Location

- S3: `s3://dantelore.data.incoming/os_code_point_open/codepo/postcode_area=<xx>/`
- Glue: `incoming.os_code_point_open_codepo`
- Format: Parquet, partitioned by `postcode_area` (lowercase leading letters, e.g. `rg`)

Always filter on `postcode_area` to limit the scan.

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| postcode | string | No | Primary; joins to house_prices_ppd, voa_rating_list_entries | Full postcode (e.g. `RG14 5RU`) |
| positional_quality_indicator | int | Yes | | Code indicating coordinate accuracy (1=within building, 10=postcode sector centroid) |
| eastings | int | Yes | | OSGB36 easting of postcode mean position (metres) |
| northings | int | Yes | | OSGB36 northing of postcode mean position (metres) |
| country_code | string | Yes | | Country code (e.g. `E92000001` = England) |
| nhs_regional_ha_code | string | Yes | | NHS Regional Health Authority code |
| nhs_ha_code | string | Yes | | NHS Health Authority code |
| admin_county_code | string | Yes | | Administrative county code |
| admin_district_code | string | Yes | | Administrative district (local authority) code |
| admin_ward_code | string | Yes | | Administrative ward code |
| postcode_area | string | No | Partition key | Lowercase leading letters of postcode (e.g. `rg`) |

## Source ETL / code

`github.com/dantelore/gov-etl`, `os_code_point_open/os_code_point_open_load.py`.

## Freshness & update cadence

OS publish Code Point Open quarterly. Re-run the loader to pick up the latest release —
it fetches the current release automatically via the OS Data Hub API.

## Known issues & caveats

- Coordinates are the mean position of all delivery points in the postcode unit, not a
  centroid of the postcode boundary. For large rural postcodes this can be misleading.
- `positional_quality_indicator` values of 60 or 90 indicate postcodes with no precise
  grid reference — treat their coordinates with caution.
- Column names use `eastings`/`northings` (plural), not `x_coordinate`/`y_coordinate`
  as used in UPRN and elevation tables.
- Coverage is Great Britain only — no Northern Ireland postcodes.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
