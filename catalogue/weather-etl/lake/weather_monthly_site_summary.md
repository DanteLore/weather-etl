# lake.weather_monthly_site_summary — Monthly Temperature Summary by Station

## Description

Percentile-based monthly temperature aggregates for each Met Office station. Provides low (5th percentile), high (95th percentile), and median (50th percentile) temperatures per station per month. Built from [`lake.weather`](weather.md). Use this for trend analysis, seasonal comparisons, or plotting temperature ranges without querying the full hourly dataset.

Note: `low_temp` and `high_temp` are 5th and 95th percentiles, not observed minimums and maximums — they are resistant to individual anomalous readings.

## Classification

Public. Derived from Met Office public data; no personal data.

## Owner

Dan (logicalgenetics@gmail.com). See the [weather-etl repo](https://github.com/dantelore/weather-etl) for the code.

## Source & references

Derived from [`lake.weather`](weather.md).

## License & usage restrictions

Met Office DataHub terms apply. Free tier available for non-commercial use.

## Location

- S3: `s3://dantelore.data.lake/weather_monthly_site_summary/`
- Glue: `lake.weather_monthly_site_summary`
- Format: Parquet (ParquetHiveSerDe)
- Not partitioned

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| site_id | string | No | Primary (with year, month) | Met Office site identifier. Joins to `lake.weather_stations.site_id` |
| site_name | string | No | | Human-readable station name |
| lat | double | No | | Station latitude, WGS84 |
| lon | double | No | | Station longitude, WGS84 |
| year | bigint | No | Primary (with site_id, month) | Year of the summary period |
| month | bigint | No | Primary (with site_id, year) | Month of the summary period |
| low_temp | double | Yes | | 5th percentile temperature in °C for the month (`approx_percentile`) |
| high_temp | double | Yes | | 95th percentile temperature in °C for the month (`approx_percentile`) |
| median_temp | double | Yes | | Median temperature in °C for the month (`approx_percentile`) |

## Source ETL / code

[`github.com/dantelore/weather-etl`](https://github.com/dantelore/weather-etl), `weather_data_model/lambda_function.py`. Lambda function `model_weather_data`. Built in the same nightly run as `lake.weather`, immediately after that table is written.

SQL summary: groups `lake.weather` by `(site_id, site_name, lat, lon, year, month)` and applies `approx_percentile(temperature, 0.05/0.50/0.95)`.

## Freshness & update cadence

Full table rebuild every night at 06:00 UTC, immediately after `lake.weather` is rebuilt in the same Lambda invocation. The entire dataset is deleted and rewritten each run.

## Known issues & caveats

- Temperature percentiles use Athena's `approx_percentile`, which is an approximation algorithm. Results are accurate to within ~1% but not exact.
- The table is not partitioned, so a full scan is required for any query. At current station count and history length this is fast, but worth noting if the dataset grows significantly.
- `low_temp` and `high_temp` reflect the 5th and 95th percentile of hourly readings — not the coldest or hottest single observation. A station with one extremely cold reading will not necessarily have a lower `low_temp` than one with consistently cold readings.
- Like `lake.weather`, this table is fully deleted and rebuilt nightly. Queries around 06:00 UTC may see an empty or partial result.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
