# incoming.midas — Raw CEDA MIDAS Open Historic Observations

## Description

Raw historic hourly weather observations from UK Met Office surface stations, sourced from the CEDA (Centre for Environmental Data Analysis) MIDAS Open archive. Covers temperature, humidity, wind, pressure, cloud, sunshine, and other meteorological variables going back decades. Sometimes referred to as "MIDAS" or "UK hourly weather obs."

This is the raw landing table, stored as-ingested from the CEDA source files. Distinct from the DataHub live feed — this is the quality-controlled historical archive. Not updated automatically; the 202107 snapshot is the current dataset.

## Classification

Public. MIDAS Open is a freely available research dataset; no personal data.

## Owner

Dan (logicalgenetics@gmail.com). See the [weather-etl repo](https://github.com/dantelore/weather-etl) for the code.

## Source & references

CEDA Data Access Portal, MIDAS Open dataset:
https://dap.ceda.ac.uk/badc/ukmo-midas-open/data/uk-hourly-weather-obs/

Dataset version used: `202107`. QC version: `qc-version-1`. CEDA registration required for access (free).

## License & usage restrictions

Open Government Licence v3.0 via CEDA. Free to use with attribution. Registration at https://services.ceda.ac.uk/ required to obtain download credentials.

## Location

- S3 raw (gzipped CSV): `s3://dantelore.data.raw/midas/`
- S3 incoming (NDJSON): `s3://dantelore.data.incoming/midas/`
- Glue: `incoming.midas`
- Format: NDJSON (JsonSerDe), one observation per line
- Partitioned by: `year` (string)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| ob_time | timestamp | No | Primary (with src_id) | Observation date and time |
| observation_station | string | No | | Station name from file header |
| lat | float | No | | Station latitude, WGS84 |
| lon | float | No | | Station longitude, WGS84 |
| id | string | Yes | | Station identifier from source |
| id_type | string | Yes | | Type of identifier |
| met_domain_name | string | Yes | | Met Office domain name |
| src_id | int | No | Primary (with ob_time) | MIDAS source station ID |
| wind_speed_unit_id | int | Yes | | Wind speed unit code |
| wind_direction | int | Yes | | Wind direction in degrees |
| wind_speed | float | Yes | | Wind speed |
| cld_ttl_amt_id | int | Yes | | Total cloud amount code |
| low_cld_type_id | int | Yes | | Low cloud type code |
| med_cld_type_id | int | Yes | | Medium cloud type code |
| hi_cld_type_id | int | Yes | | High cloud type code |
| cld_base_amt_id | int | Yes | | Cloud base amount code |
| cld_base_ht | int | Yes | | Cloud base height |
| visibility | int | Yes | | Visibility in metres |
| msl_pressure | float | Yes | | Mean sea level pressure |
| vert_vsby | int | Yes | | Vertical visibility |
| air_temperature | float | Yes | | Air temperature in °C |
| dewpoint | float | Yes | | Dew point temperature in °C |
| wetb_temp | float | Yes | | Wet bulb temperature in °C |
| rltv_hum | float | Yes | | Relative humidity |
| stn_pres | float | Yes | | Station pressure |
| alt_pres | float | Yes | | Altimeter pressure |
| ground_state_id | string | Yes | | Ground state code |
| q10mnt_mxgst_spd | int | Yes | | 10-minute maximum gust speed |
| cavok_flag | string | Yes | | CAVOK flag (Ceiling And Visibility OK) |
| cs_hr_sun_dur | float | Yes | | Campbell-Stokes hourly sunshine duration |
| wmo_hr_sun_dur | float | Yes | | WMO hourly sunshine duration |
| snow_depth | int | Yes | | Snow depth |
| wind_direction_q | int | Yes | | Wind direction quality flag |
| wind_speed_q | int | Yes | | Wind speed quality flag |
| meto_stmp_time | string | Yes | | Met Office stamp time |
| drv_hr_sun_dur | float | Yes | | Derived hourly sunshine duration |
| year | string | No | Partition key | Partition: year of observation |

## Source ETL / code

[`github.com/dantelore/weather-etl`](https://github.com/dantelore/weather-etl), `ceda_bulk_data/bulkdata.py`. One-off bulk load script; not on a recurring schedule. Crawls the CEDA DAP HTTP directory tree and downloads all files matching `qc-version-1`.

## Freshness & update cadence

Bulk-loaded once in January 2022 from the CEDA 202107 dataset release. Covers observations from 1875 through end of 2020. Not automatically updated and no further updates are planned.

## Known issues & caveats

- The CEDA source files use a non-standard format: CSV data is embedded between `data` / `end data` markers with metadata headers above. The ETL parses this custom format.
- Only `qc-version-1` files are loaded; other QC versions in the archive are skipped to avoid duplicates.
- Many fields are sparse — rows only contain fields that had values in the source. Absence of a field means it was not reported, not necessarily that it was zero.
- CEDA credentials (username/password) are required to re-run the bulk load; they are not stored in the repo.
- Cloud and sunshine fields use Met Office numeric codes; refer to the MIDAS documentation for decode tables.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Deprecated. Static snapshot covering 1875–2020, bulk-loaded in January 2022 from the CEDA 202107 release. No further updates are planned. The data remains in S3 and is queryable, but is not maintained.

Note: Glue partitions for this table are not registered. Run `MSCK REPAIR TABLE incoming.midas` in Athena before querying, otherwise all queries will return zero rows.
