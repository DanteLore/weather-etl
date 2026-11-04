# incoming.webanalytics

## Description

Raw CloudFront access logs for dantelore.com and associated domains, written directly from the CDN to S3 and partitioned for querying in Athena. Each row is one HTTP request, including the client IP, URI, HTTP status, bytes transferred, cache result, and timing data. This is the unprocessed source table — prefer `lake.webanalytics` for analysis, which adds geolocation, user-agent parsing, and bot detection on top of these fields.

## Classification

Internal. Traffic data for a personal site; no PII is collected by design (no logins, no user accounts). IP addresses are present, which are considered personal data under GDPR — do not export or share raw IP fields.

## Owner

Dan (analytics-etl). Query via `incoming.webanalytics` in Athena.

## Source & references

- AWS CloudFront CDN serving dantelore.com
- Log format: [CloudFront standard access log format](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/AccessLogs.html) (tab-delimited, two header lines per file)
- CloudFront writes log files every hour; each file covers roughly one hour of traffic

## License & usage restrictions

None. This is self-generated traffic data from a personal website.

## Location

- S3: `s3://dantelore.data.incoming/webanalytics/year={yyyy}/month={mm}/day={dd}/`
- Glue / Athena: `incoming.webanalytics`
- Format: TSV (tab-delimited CloudFront format), partitioned by year/month/day (int)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| date | string | No | Primary (with time) | Log date (YYYY-MM-DD) |
| time | string | No | Primary (with date) | UTC time (HH:MM:SS) |
| location | string | No | | CloudFront edge location code (e.g. LHR61) |
| bytes | bigint | No | | Bytes transferred to the client |
| request_ip | string | No | | Client IP address (IPv4 or IPv6) |
| method | string | No | | HTTP method (GET, POST, HEAD, etc.) |
| host | string | No | | CloudFront distribution domain (not the user-facing domain) |
| uri | string | No | | Full URI path including query string |
| status | int | No | | HTTP status code returned to the client |
| referrer | string | Yes | | HTTP Referer header; `-` if not set |
| user_agent | string | Yes | | User-Agent header |
| query_string | string | Yes | | Query string portion of the URI; `-` if none |
| cookie | string | Yes | | Cookie header; `-` if not set |
| result_type | string | No | | CloudFront cache result (Hit, Miss, Error, LimitExceeded, etc.) |
| request_id | string | No | | Unique CloudFront request identifier |
| host_header | string | No | | Host header sent by the client (the user-facing domain) |
| request_protocol | string | No | | HTTP or HTTPS |
| request_bytes | bigint | No | | Bytes sent by the client in the request |
| time_taken | float | No | | Seconds from request received to last byte sent |
| xforwarded_for | string | Yes | | X-Forwarded-For header; may contain multiple IPs |
| ssl_protocol | string | Yes | | TLS protocol version (e.g. TLSv1.3); `-` for HTTP |
| ssl_cipher | string | Yes | | TLS cipher suite; `-` for HTTP |
| response_result_type | string | No | | CloudFront response result type (may differ from result_type for ranged GETs) |
| http_version | string | No | | HTTP version (HTTP/1.0, HTTP/1.1, HTTP/2.0, HTTP/3.0) |
| fle_status | string | Yes | | Field-level encryption status; `-` if not configured |
| fle_encrypted_fields | int | Yes | | Count of encrypted fields; `-` if not configured |
| c_port | int | No | | Client TCP port |
| time_to_first_byte | float | No | | Seconds from request received to first byte sent |
| x_edge_detailed_result_type | string | No | | Detailed CloudFront result type (e.g. Hit, Miss, OriginShieldHit) |
| sc_content_type | string | Yes | | Content-Type header from the origin response |
| sc_content_len | bigint | Yes | | Content-Length header from the origin response |
| sc_range_start | bigint | Yes | | Start byte for range requests; `-` otherwise |
| sc_range_end | bigint | Yes | | End byte for range requests; `-` otherwise |
| year | int | No | Partition | Log year |
| month | int | No | Partition | Log month (1-12) |
| day | int | No | Partition | Log day (1-31) |

## Source ETL / code

CloudFront writes directly to S3 — no ETL code for the log files themselves. A `partitioner-lambda` (see `analytics/partitioner-lambda/`) receives S3 event notifications and reorganises incoming files into the `year=/month=/day=` prefix structure so Athena can use partition pruning.

## Freshness & update cadence

CloudFront writes log files roughly every hour. The `partitioner-lambda` moves each file immediately on arrival via S3 event notification, so the lag between a request happening and it appearing in Athena is typically 1-2 hours.

History from: 2025-01-01.

## Known issues & caveats

- CloudFront logs use `-` (not NULL) to represent absent or not-applicable field values. Athena will return these as the string `"-"`, not as SQL NULL. Filter accordingly.
- Two tab-delimited header lines appear at the top of each raw log file (a version line and a field-names line). The Glue table definition skips these, but be aware if reading raw files from S3 directly.
- The `host` field is the CloudFront distribution domain (e.g. `d1abc123.cloudfront.net`), not the user-facing hostname. Use `host_header` to identify which site a request was for.
- `xforwarded_for` may contain multiple comma-separated IPs when a request passes through intermediary proxies. The leftmost IP is usually the original client.
- `sc_range_start` and `sc_range_end` return `-1` (not NULL) when not applicable.
- Partition columns are stored as integers.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|
| lake.webanalytics | Dan | Hourly enrichment Lambda reads from this table |

## Status

Active.
