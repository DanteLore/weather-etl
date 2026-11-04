# incoming.weather — Raw Met Office Hourly Observations

## Description

Raw hourly weather observations from 137 UK and Ireland Met Office surface stations, as received from the Met Office DataHub API. Covers wind speed, wind direction, wind gust speed, temperature, dew point, pressure, humidity, visibility, and weather type. Written directly from the API response with no transformation beyond flattening. Sometimes referred to as "WxObs" or "surface observations."

This is the raw landing table. For analysis use [`lake.weather`](../lake/weather.md), which is deduplicated, filtered for known bad data, and stored as Parquet.

## Classification

Public. Sourced from the Met Office public data API; no personal data.

## Owner

Dan. See the [weather-etl repo](https://github.com/dantelore/weather-etl) for the code.

## Source & references

Met Office DataHub API. Observations endpoint: `val/wxobs/all/json/all`. Requires an API key registered at https://datahub.metoffice.gov.uk/

## License & usage restrictions

Met Office DataHub terms apply. Free tier available for non-commercial use. Check the current licence before using in any commercial product.

## Location

- S3: `s3://dantelore.data.incoming/weather/`
- Glue: `incoming.weather`
- Format: NDJSON (JsonSerDe), one observation per line
- Partitioned by: `year` (string), `month` (string), `day` (string)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| observation_ts | timestamp | No | Primary (with site_id) | Observation timestamp |
| site_id | string | No | Primary (with observation_ts) | Met Office site identifier. Joins to `lake.weather_stations.site_id` |
| site_name | string | No | | Human-readable station name |
| site_country | string | No | | Country the station is in |
| site_continent | string | No | | Continent the station is in |
| site_elevation | double | No | | Station elevation in metres |
| lat | double | No | | Station latitude, WGS84 |
| lon | double | No | | Station longitude, WGS84 |
| wind_direction | string | Yes | | Wind direction as a compass bearing string |
| wind_speed | double | Yes | | Wind speed |
| screen_relative_humidity | double | Yes | | Relative humidity at screen level |
| pressure | double | Yes | | Atmospheric pressure |
| pressure_tendency | string | Yes | | Pressure tendency (rising/falling/steady) |
| temperature | double | Yes | | Air temperature in °C |
| dew_point | double | Yes | | Dew point temperature in °C |
| visibility | int | Yes | | Visibility in metres |
| weather_type | int | Yes | | Met Office weather type code |
| year | string | No | Partition key | Partition: year of observation |
| month | string | No | Partition key | Partition: month of observation |
| day | string | No | Partition key | Partition: day of observation |

## Source ETL / code

[`github.com/dantelore/weather-etl`](https://github.com/dantelore/weather-etl), `datahub_etl/weather_etl.py`. Lambda function `load_weather_data`, triggered hourly by CloudWatch Events.

## Freshness & update cadence

Written every hour by the `load_weather_data` Lambda (CloudWatch `rate(1 hour)`). The DataHub API returns the last 24 hours of observations on each call, so a single missed run does not create a gap — the next run will backfill. In normal operation data is no more than ~1 hour stale.

## Known issues & caveats

- CHIVENOR station produced a run of implausible temperature readings in July 2022 (below −5°C). These are excluded in `lake.weather` but are present here in the raw table.
- The API can return multiple observations within the same hour for the same site. Duplicates are not removed here; deduplication happens in `lake.weather` using a window function keyed on `(hour(observation_ts), site_id)`.
- Some sites return no observations for a given period; these are absent from the table rather than present as nulls.
- The ETL was originally written against the older DataPoint API. The legacy `datapoint_etl/` module is still in the repo but inactive; all current writes come from `datahub_etl/`.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|
| lake.weather | — | Nightly rebuild reads all of incoming.weather to produce the deduplicated lake table |

## Status

Active.
