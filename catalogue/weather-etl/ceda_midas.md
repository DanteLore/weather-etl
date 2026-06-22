# CEDA MIDAS Open - UK Hourly Weather Observations

## Description

Historic hourly weather observations from UK Met Office surface stations,
sourced from the CEDA (Centre for Environmental Data Analysis) MIDAS Open
archive. Covers temperature, humidity, wind, pressure, and other meteorological
variables going back decades. Sometimes referred to as "MIDAS" or
"UK hourly weather obs." Distinct from the DataPoint/DataHub live feed - this
is the quality-controlled historical archive.

## Classification

Public. MIDAS Open is a freely available research dataset; no personal data.

## Owner

Dan. See weather-etl repo for the code.

## Source & references

CEDA Data Access Portal, MIDAS Open dataset:
https://dap.ceda.ac.uk/badc/ukmo-midas-open/data/uk-hourly-weather-obs/

Dataset version used: 202107. QC version: `qc-version-1`.

CEDA registration required for access (free).

## License & usage restrictions

Open Government Licence v3.0 via CEDA. Free to use with attribution.
Registration at https://services.ceda.ac.uk/ required to obtain credentials.

## Location

- S3 raw (gzipped CSV): `s3://dantelore.data.raw/midas/`
- S3 incoming (gzipped NDJSON): `s3://dantelore.data.incoming/midas/year=<YYYY>/`
- Format: NDJSON, one observation per line, partitioned by year

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| observation_station | string | No | | Met Office station name from file header |
| lat | decimal | No | | Station latitude, WGS84 |
| lon | decimal | No | | Station longitude, WGS84 |
| height | string | Yes | | Station height from file header |
| county | string | Yes | | Historic county name from file header |
| date_valid | string | No | Primary (with station) | Observation date/time |

Additional observation fields vary by station and data file. Fields containing
only `NA` in the source are dropped. Refer to CEDA documentation for the full
MIDAS field list.

## Source ETL / code

`github.com/dantelore/weather-etl`, `ceda_bulk_data/bulkdata.py`.
One-off bulk load script; not on a recurring schedule. Crawls the CEDA DAP
HTTP directory tree and downloads all files matching `qc-version-1`.

## Freshness & update cadence

Bulk-loaded once from the 202107 dataset version. Not automatically updated.
A manual re-run would be needed to pick up newer CEDA dataset releases.

## Known issues & caveats

- The CEDA source files use a non-standard format: CSV data is embedded between
  `data` / `end data` markers with metadata headers above. The ETL parses this
  custom format.
- Fields with value `NA` in the source are omitted from the output rows rather
  than stored as nulls, so the field set is sparse and varies per row.
- Only `qc-version-1` is loaded; other QC versions in the archive are skipped
  to avoid duplicates.
- CEDA credentials (username/password) are required at runtime; they are not
  stored in the repo.
- Bulk load is slow due to the size of the archive and the crawl-then-download
  approach.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active (static snapshot - 202107 release only).
