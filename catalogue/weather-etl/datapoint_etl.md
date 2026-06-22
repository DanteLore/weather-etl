# Met Office DataPoint / DataHub - Hourly Weather Observations

## Description

Hourly weather observations from Met Office surface stations across the UK and
worldwide. Covers wind speed, wind direction, wind gust, temperature, dew point,
pressure, humidity, visibility, and weather type. Originally fetched from the
Met Office DataPoint API (now migrated to the DataHub API). Sometimes referred
to as "WxObs" or "surface observations."

## Classification

Public. Sourced from the Met Office public data API; no personal data.

## Owner

Dan. See weather-etl repo for the code.

## Source & references

Met Office DataHub API (previously DataPoint API). Observations endpoint:
`val/wxobs/all/json/all`. Requires an API key registered at
https://datahub.metoffice.gov.uk/

## License & usage restrictions

Met Office DataHub terms apply. Free tier available for non-commercial use.
Check current licence before using in any commercial product.

## Location

- S3: `s3://dantelore.data.incoming/weatherData/`
- Format: NDJSON, one observation per line
- Raw JSON from the API is also retained at `s3://dantelore.data.incoming/weatherData/datahub_raw.json`

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| observation_ts | string | No | Primary (with site_id) | Observation timestamp, format `YYYY-MM-DD HH:MM:SS` |
| site_id | string | No | Primary (with observation_ts) | Met Office site identifier |
| site_name | string | No | | Human-readable site name |
| site_country | string | No | | Country the site is in |
| site_continent | string | No | | Continent the site is in |
| site_elevation | decimal | No | | Site elevation in metres |
| lat | decimal | No | | Site latitude, WGS84 |
| lon | decimal | No | | Site longitude, WGS84 |
| wind_direction | string | Yes | | Wind direction (compass bearing as string) |
| wind_gust | string | Yes | | Wind gust speed |
| wind_speed | string | Yes | | Wind speed |
| screen_relative_humidity | string | Yes | | Relative humidity at screen level |
| pressure | string | Yes | | Atmospheric pressure |
| pressure_tendency | string | Yes | | Pressure tendency (rising/falling/steady) |
| temperature | string | Yes | | Air temperature |
| dew_point | string | Yes | | Dew point temperature |
| visibility | string | Yes | | Visibility |
| weather_type | string | Yes | | Met Office weather type code |

Note: most observation fields are typed as string in the schema even though they
contain numeric values - the Met Office API returns them as strings and they are
stored as-is.

## Source ETL / code

`github.com/dantelore/weather-etl`, `datapoint_etl/weather_etl.py` and
`datahub_etl/weather_etl.py`. Lambda function triggered on a schedule.

## Freshness & update cadence

Runs on a Lambda schedule. The Met Office observations endpoint returns data for
the last 24 hours; the ETL is designed to run at least daily to avoid gaps.

## Known issues & caveats

- The Met Office API returns `Period` and `Rep` as either a single object or a
  list depending on how many observations are present for that site/day. The ETL
  handles this but it's an odd quirk of the source API.
- Most numeric fields (temperature, wind speed, etc.) are stored as strings,
  not numerics. Cast them at query time.
- Some sites return no observations for a given period; these are logged and
  skipped rather than inserted as nulls.
- The ETL was originally written against the DataPoint API and migrated to
  DataHub; the legacy `datapoint_etl/` module is still present but the active
  code is in `datahub_etl/`.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
