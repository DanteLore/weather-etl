# lake.webanalytics

## Description

Enriched web analytics for dantelore.com and associated domains, derived hourly from raw CloudFront access logs. Each row is one HTTP request, with geolocation (city, country, coordinates, ISP), user-agent parsing (browser, OS, device type, bot detection), canonical domain resolution, and a flag indicating whether the request was for a content page or a static asset. This is the production-ready table for analysis and public-facing charts — use it in preference to `incoming.webanalytics` for anything beyond raw log inspection.

## Classification

Internal. Contains IP addresses (personal data under GDPR) and derived geolocation; do not export or share individual-request data. Aggregate queries (counts, percentages, summaries) are fine to publish.

## Owner

Dan (analytics-etl). Query via `lake.webanalytics` in Athena.

## Source & references

- Source table: `incoming.webanalytics` (raw CloudFront logs)
- Geolocation: [ipinfo.io](https://ipinfo.io/) REST API (requires API token)
- User-agent parsing: Python `user-agents` library (ua-parser based)
- Domain mapping: AWS CloudFront API + `tldextract`

## License & usage restrictions

None on the traffic data itself (self-generated). ipinfo.io data is used under their API terms — do not re-export raw geo fields in a form that reconstructs their database.

## Location

- S3: `s3://dantelore.data.lake/webanalytics/year={yyyy}/month={mm}/`
- Glue / Athena: `lake.webanalytics`
- Format: Parquet (ParquetHiveSerDe), partitioned by year/month (int)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| date | string | No | Primary (with time) | Log date (YYYY-MM-DD) |
| time | string | No | Primary (with date) | UTC time (HH:MM:SS) |
| location | string | No | | CloudFront edge location code (e.g. LHR61) |
| bytes | bigint | No | | Bytes transferred to the client |
| request_ip | string | No | | Client IP address (IPv4 or IPv6) |
| method | string | No | | HTTP method (GET, POST, HEAD, etc.) |
| uri | string | No | | Full URI path including query string |
| status | int | No | | HTTP status code returned to the client |
| referrer | string | Yes | | HTTP Referer header; `-` if not set |
| user_agent | string | Yes | | Raw User-Agent string |
| query_string | string | Yes | | Query string portion of the URI; `-` if none |
| result_type | string | No | | CloudFront cache result (Hit, Miss, Error, etc.) |
| host_header | string | No | | User-facing hostname from the request Host header |
| request_protocol | string | No | | HTTP or HTTPS |
| request_bytes | bigint | No | | Bytes sent by the client |
| time_taken | float | No | | Seconds from request received to last byte sent |
| xforwarded_for | string | Yes | | X-Forwarded-For header |
| response_result_type | string | No | | CloudFront response result type |
| http_version | string | No | | HTTP version (HTTP/1.1, HTTP/2.0, etc.) |
| c_port | int | No | | Client TCP port |
| time_to_first_byte | float | No | | Seconds from request received to first byte sent |
| x_edge_detailed_result_type | string | No | | Detailed CloudFront result type |
| sc_content_type | string | Yes | | Content-Type from the origin response |
| sc_content_len | bigint | Yes | | Content-Length from the origin response |
| sc_range_start | bigint | Yes | | Start byte for range requests; -1 otherwise |
| sc_range_end | bigint | Yes | | End byte for range requests; -1 otherwise |
| geo_lat | float | Yes | | Client latitude from ipinfo.io; null if lookup failed |
| geo_lon | float | Yes | | Client longitude from ipinfo.io; null if lookup failed |
| geo_city | string | Yes | | City name from ipinfo.io; empty string if lookup failed |
| geo_country | string | Yes | | ISO 3166-1 alpha-2 country code from ipinfo.io; empty string if lookup failed |
| geo_org | string | Yes | | ISP or organisation name from ipinfo.io (e.g. "AS15169 Google LLC") |
| domain | string | Yes | | Canonical domain resolved from host_header (e.g. `dantelore.com`). Falls back to raw host_header if not found in CloudFront distribution list. |
| visitor_type | string | Yes | | `Bot` or `Human` based on user-agent parsing |
| device_type | string | Yes | | `Mobile`, `Tablet`, or `Desktop` based on user-agent parsing |
| browser | string | Yes | | Browser family (e.g. Chrome, Firefox, Safari, Python Requests) |
| os | string | Yes | | OS family (e.g. Windows, macOS, Linux, iOS, Android) |
| is_post_content | bool | Yes | | True if the URI looks like a content page (no file extension after stripping query string); false for assets (JS, CSS, images, fonts, etc.) |
| year | int | No | Partition | Log year |
| month | int | No | Partition | Log month (1-12) |

## Source ETL / code

`analytics/modelling-lambda/` in this repo. An EventBridge rule triggers the Lambda hourly. Each run:

1. Finds the latest datetime already in `lake.webanalytics`
2. Fetches up to 2 hours of new rows from `incoming.webanalytics`
3. Geolocates unique IPs via ipinfo.io (with a within-month cache built from existing lake data to minimise API calls)
4. Resolves `host_header` to a canonical domain via the CloudFront API
5. Parses each user-agent string for bot/device/browser/OS
6. Writes enriched rows as a Parquet file to `s3://dantelore.data.lake/webanalytics/year={y}/month={m}/`

Backfill script: `analytics/backfill_webanalytics.py` — reprocesses month-by-month from scratch, using a local `.geo_cache.json` to avoid hammering the ipinfo.io API.

## Freshness & update cadence

Updated hourly via EventBridge. Each run covers the window from the last processed datetime up to 2 hours ahead, so data is typically 1-3 hours behind real time (1 hour CloudFront lag + up to 1 hour Lambda cadence).

History from: 2025-01-01.

## Known issues & caveats

- Geolocation fields (`geo_*`) are empty strings (not NULL) when an IP lookup fails or times out. Filter with `geo_country != ''` rather than `geo_country IS NOT NULL`.
- Bot detection relies on the `user-agents` library's UA parser. Sophisticated bots that spoof browser UA strings will be classified as `Human`. Conversely, some legitimate automation tools (curl, Python Requests, AWS health checks) will correctly appear as `Bot`.
- `domain` falls back to the raw `host_header` value for any CloudFront distribution not found in the account's distribution list at the time the Lambda ran. This is rare but can produce CloudFront `.cloudfront.net` hostnames in the domain field.
- `is_post_content` uses a simple heuristic: any URI whose last path segment contains a `.` is treated as an asset. This correctly handles `.html`, `.js`, `.css`, `.jpg` etc. but will misclassify unusual URLs that include a dot in a directory name.
- Partition columns are integers; month is not zero-padded.
- Several low-traffic fields from the raw CloudFront format (cookie, fle_status, fle_encrypted_fields, ssl_protocol, ssl_cipher) were dropped from the lake schema to reduce Parquet file size.
- The geo cache is scoped to the current calendar month. IPs first seen in a prior month are not pre-cached and will trigger fresh ipinfo.io lookups on their next appearance.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|
| dantelore.com/analytics | Dan | Public analytics page, charts driven by SQL queries in `charts/queries/public/` |

## Status

Active.
