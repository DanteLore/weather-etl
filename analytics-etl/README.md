# analytics-etl

Datasets produced by the analytics pipeline in the `analytics/` folder of the `dantelore` repo. Covers web traffic for dantelore.com and associated domains, sourced from AWS CloudFront access logs.

Raw logs land in the `incoming` Athena database. An hourly Lambda enriches them with geolocation and user-agent data and writes the result to the `lake` database as Parquet.

## How data flows

```
CloudFront (dantelore.com)
  |  (writes log files every hour)
  v
s3://dantelore.data.incoming/webanalytics/   [partitioner-lambda reorganises into year/month/day]
  |
  v
incoming.webanalytics   (raw TSV, 33 fields)
  |  [dantelore-analytics-modelling Lambda, runs hourly]
  |  - geo-enriches IPs via ipinfo.io
  |  - resolves domain via CloudFront API
  |  - parses user-agent (bot, device, browser, OS)
  |  - flags content vs. asset requests
  v
lake.webanalytics   (Parquet, 37 fields)
  |
  v
Public charts at dantelore.com/analytics
```

## Datasets

| Dataset | Table | Description | Cadence |
|---|---|---|---|
| [incoming/webanalytics](incoming/webanalytics.md) | `incoming.webanalytics` | Raw CloudFront access logs, partitioned by year/month/day | Continuous (hourly files) |
| [lake/webanalytics](lake/webanalytics.md) | `lake.webanalytics` | Enriched web analytics with geo, UA parsing and bot detection | Hourly Lambda |

## Key things to know

- **Use `lake.webanalytics` for analysis.** The incoming table is the raw source; the lake table adds all enrichment.
- **Partition columns are integers** (not zero-padded strings, unlike the energy-etl tables).
- **CloudFront `-` values are strings, not NULLs** in the incoming table. The lake table inherits this for fields that pass through unchanged (e.g. `referrer`).
- **Filter bots** with `WHERE visitor_type = 'Human'` for human traffic analysis. The public charts page does this by default.
- **`geo_country` empty string means geo lookup failed**, not that the visitor has no country. Filter with `geo_country != ''` rather than `IS NOT NULL`.
