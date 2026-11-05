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

## Running costs

Estimated from measurements taken 2026-11-05, at AWS eu-west-1 list prices in USD, excluding tax and the free tier:

| Component | Basis | Price |
|---|---|---|
| S3 storage | Size of each table's S3 location | $0.023 per GB-month |
| S3 writes / reads | New files and Athena result files; objects read by nightly rebuilds | $0.005 per 1,000 PUTs; $0.0004 per 1,000 GETs |
| Lambda | Invocations × average duration × memory (CloudWatch, last 30 days) | $0.0000166667 per GB-second + $0.20 per million requests |
| Athena | Bytes scanned by each pipeline query (Athena query history, last 30 days); 10 MB minimum per query; DDL such as `ADD PARTITION` is free | $5 per TB scanned |
| CloudWatch Logs | Log ingestion per Lambda (CloudWatch, last 30 days) | $0.57 per GB |

Lambdas that load several tables are split evenly between them; a model Lambda is split by bytes scanned per table. The free tier currently covers all Lambda usage, so Lambda is billed at about $0 in practice. Each entry's **Running costs (estimated)** section has the per-table breakdown.

| Dataset | Layer | Est. $/month |
|---|---|---|
| [webanalytics](incoming/webanalytics.md) | incoming | $0.641 |
| [webanalytics](lake/webanalytics.md) | lake | $0.129 |
| **Total** | | **$0.77** |

| Lambda | Memory | Runs / 30 days | Avg duration | Errors / 30 days | Est. $/month (compute + logs) |
|---|---|---|---|---|---|
| `dantelore-webanalytics-partitioner` | 128 MB | 103,177 | 0.2 s | 77,385 | $0.116 |
| `dantelore-analytics-modelling` | 256 MB | 720 | 16.9 s | 0 | $0.051 |
