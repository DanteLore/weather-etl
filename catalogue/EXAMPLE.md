# UK Postcode Lookup

## Description

Lookup table mapping every live UK postcode to its administrative geography
(local authority, ward, parliamentary constituency) and approximate
latitude/longitude centroid. Use this to answer "what local authority is
postcode X in" or to plot postcodes on a map. Sourced from the ONS Postcode
Directory (ONSPD), sometimes referred to as the "postcode-to-geography" file.

## Classification

Public. Sourced entirely from an open government dataset; contains no
personal or customer data, only postcode-level geography.

## Owner

Dan (data platform). Ping me on Slack if something looks wrong before
assuming the source data is wrong - I've broken the import before.

## Source & references

Office for National Statistics, ONS Postcode Directory (ONSPD), published
quarterly. Download page: https://geoportal.statistics.gov.uk/ (search
"ONS Postcode Directory"). Licensed under the Open Government Licence.

## License & usage restrictions

Open Government Licence v3.0. Free to use, share and adapt, including
commercially, with attribution to ONS. No restriction on internal use.

## Location

- S3: `s3://example-data-lake/reference/postcode_lookup/`
- Glue: `reference_db.postcode_lookup`
- Format: Parquet, partitioned by `onspd_release` (e.g. `2026-05`)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| postcode | string | No | Primary | Full UK postcode, no internal space (e.g. `RG189AB`) |
| postcode_formatted | string | No | | Same postcode with standard spacing (e.g. `RG18 9AB`) |
| local_authority_code | string | No | Joins to `reference_db.local_authorities.la_code` | ONS local authority district code (GSS code) |
| local_authority_name | string | No | | Local authority name at time of release |
| ward_code | string | Yes | | ONS ward code, null for a small number of non-residential postcodes |
| constituency_name | string | No | | Westminster parliamentary constituency, post-2024 boundaries |
| latitude | decimal | Yes | | Centroid latitude, WGS84. Null for around 0.1% of postcodes with no grid reference in source data |
| longitude | decimal | Yes | | Centroid longitude, WGS84 |
| onspd_release | string | No | Partition key | Release version this row came from, e.g. `2026-05` |

## Source ETL / code

`github.com/example-org/reference-data-etl`, `etl/postcode_lookup.py`. Runs as
a one-off script triggered manually after each ONSPD quarterly release, not on
a schedule, since the source only updates quarterly and there's no API to
poll.

## Freshness & update cadence

Refreshed manually, roughly quarterly, in line with ONS's own release
schedule. There is no automatic check that we've picked up the latest
release - if precision matters, check the `onspd_release` partition against
the current ONSPD version on the ONS site before relying on this for
anything time-sensitive.

## Known issues & caveats

- Around 0.1% of postcodes (mostly very new or non-residential ones) have no
  latitude/longitude in the source data. These come through as null rather
  than zero or a default - don't assume null means "in the sea."
- Postcodes are retired and reused by Royal Mail over long periods. This
  table only reflects the latest ONSPD release; it is not a historical
  record of which postcode meant what, when.
- Ward boundaries changed significantly in the 2023-24 boundary review.
  Joining this table to older datasets that use pre-2023 ward codes will
  silently produce wrong results rather than an error, since both look like
  valid ward codes.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|
| Customer geography dashboard | Priya, #analytics | Joins customer postcodes to local authority for regional reporting |
| Field sales territory tool | Field Ops team | Uses lat/long centroids for territory mapping; sensitive to the null coordinate issue above |

## Status

Active.
