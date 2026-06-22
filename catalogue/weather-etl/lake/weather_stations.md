# lake.weather_stations — Met Office Station Metadata

## Description

Metadata for the 137 UK and Ireland Met Office surface monitoring stations used by this pipeline. Includes name, country, elevation, and coordinates. Used to enrich observation records and to drive the DataHub API calls via the geohash cache. The source of truth is `datahub_etl/sites.json` in the ETL repo.

## Classification

Public. Station metadata sourced from the Met Office; no personal data.

## Owner

Dan (logicalgenetics@gmail.com). See the [weather-etl repo](https://github.com/dantelore/weather-etl) for the code.

## Source & references

Met Office DataHub API station list, curated into `datahub_etl/sites.json` in the ETL repo. Updated manually when stations are added or removed.

## License & usage restrictions

Met Office DataHub terms apply. Station metadata is not subject to additional restrictions beyond the DataHub licence.

## Location

- S3: `s3://dantelore.data.lake/weather_stations/`
- Glue: `lake.weather_stations`
- Format: JSON (JsonSerDe)
- Not partitioned

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| site_id | string | No | Primary | Met Office site identifier. Joins to `incoming.weather.site_id` and `lake.weather.site_id` |
| site_name | string | No | | Human-readable station name |
| site_country | string | No | | Country the station is in |
| site_elevation | double | No | | Station elevation in metres |
| lat | double | No | | Station latitude, WGS84 |
| lon | double | No | | Station longitude, WGS84 |

## Source ETL / code

[`github.com/dantelore/weather-etl`](https://github.com/dantelore/weather-etl), `datahub_etl/site_loader.py`. The `sites.json` file is uploaded to S3 as part of the `build.sh` deploy script and is also loaded at Lambda runtime via `site_loader.py` (with an Athena fallback when `USE_ATHENA_SITES=true`).

## Freshness & update cadence

Updated on each deployment (`bash build.sh`), which uploads `datahub_etl/sites.json` to S3. There is no automatic scheduled update. In practice this changes rarely — only when Met Office stations are added, renamed, or decommissioned.

## Known issues & caveats

- The Terraform description says 137 stations; CLAUDE.md says 135. The Terraform is more recently edited — treat 137 as current. Verify against `datahub_etl/sites.json` for exact count.
- There is no `site_continent` field in this table; that field is populated directly on observation records in `incoming.weather` and `lake.weather` at ETL time.
- Station metadata is denormalised into every observation row in `incoming.weather` and `lake.weather`, so joining to this table is rarely needed for standard queries.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|
| incoming.weather / lake.weather | — | Site metadata is denormalised into every observation row at ETL time |

## Status

Active.
