# lake.weather — Deduplicated Met Office Hourly Observations

## Description

Cleaned and deduplicated hourly weather observations from 137 UK and Ireland Met Office surface stations. This is the main analysis table. Derived from [`incoming.weather`](../incoming/weather.md) by removing duplicates within each station-hour and filtering out a known run of bad readings from Chivenor. Covers wind speed, wind direction, temperature, dew point, pressure, humidity, visibility, and weather type.

## Classification

Public. Derived from Met Office public data; no personal data.

## Owner

Dan. See the [weather-etl repo](https://github.com/dantelore/weather-etl) for the code.

## Source & references

Derived from [`incoming.weather`](../incoming/weather.md), which is sourced from the Met Office DataHub API.

## License & usage restrictions

Met Office DataHub terms apply. Free tier available for non-commercial use. Check the current licence before using in any commercial product.

## Location

- S3: `s3://dantelore.data.lake/weather/`
- Glue: `lake.weather`
- Format: Parquet (ParquetHiveSerDe)
- Partitioned by: `year` (bigint), `month` (bigint)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| observation_ts | timestamp | No | Primary (with site_id) | Observation timestamp, deduplicated to one row per station per hour |
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
| year | bigint | No | Partition key | Partition: year of observation |
| month | bigint | No | Partition key | Partition: month of observation |

## Source ETL / code

[`github.com/dantelore/weather-etl`](https://github.com/dantelore/weather-etl), `weather_data_model/lambda_function.py`. Lambda function `model_weather_data`.

The rebuild SQL deduplicates using `ROW_NUMBER() OVER (PARTITION BY date_trunc('hour', observation_ts), site_id ORDER BY observation_ts DESC)` — keeping the latest observation within each station-hour. It also excludes Chivenor readings below −5°C in July 2022.

## Freshness & update cadence

Full table rebuild every night at 06:00 UTC by the `model_weather_data` Lambda (`cron(0 6 * * ? *)`). The entire Parquet dataset is deleted and rewritten from `incoming.weather` on each run. Data is typically complete and current by ~06:05 UTC each morning.

Because the rebuild reads all of `incoming.weather` every time, corrections to past incoming data will automatically propagate without any manual intervention.

## Running costs (estimated)

Estimated monthly cost in USD at AWS eu-west-1 list prices, from measurements taken 2026-11-05: the S3 size of this table's location, plus the last 30 days of Lambda metrics and Athena query history. Excludes tax and the AWS free tier (which currently covers all Lambda usage). Method: [weather-etl README](../README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.078 GB, 960 objects | $0.002 |
| Lambda `model_weather_data` (99% share) | 46 rebuilds/month | $0.006 |
| Athena (rebuild) | 46 × 2,584 MB scanned of `incoming.weather` | $0.594 |
| S3 reads (rebuild) | 46 × 7,734 objects | $0.142 |
| CloudWatch Logs | Lambda log ingestion | $0.003 |
| **Total** | | **$0.75** |

Querying it: a full scan reads about 0.08 GB, roughly <$0.001 per query (Athena bills a 10 MB minimum).

## Known issues & caveats

- CHIVENOR July 2022: a run of implausible sub-−5°C readings is hard-filtered out in the rebuild SQL. The raw data is still present in `incoming.weather` if you need it.
- Deduplication keeps the *latest* observation timestamp within each station-hour, not the first. In practice the difference is negligible but worth knowing if timestamp precision matters.
- The table is fully deleted and rebuilt nightly, so any Athena query running against it around 06:00 UTC may see an empty or partial result. Queries should be scheduled to run after the rebuild completes.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|
| lake.weather_monthly_site_summary | — | Built from this table in the same nightly Lambda run |

## Status

Active.
