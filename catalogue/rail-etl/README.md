# rail-etl datasets

UK rail performance data, focused initially on the Thatcham (THA) - Reading (RDG) /
London Paddington (PAD) GWR route. See [BRIEF.md](https://github.com/DanteLore/rail-etl/blob/master/BRIEF.md) for the full
source survey and rationale.

| Dataset | Table | Description |
|---|---|---|
| [rtt_services](rtt_services.md) | `incoming.rtt_services` | RTT actual-vs-scheduled times, cancellations and formation for THA/RDG/PAD |

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
| [rtt_services](rtt_services.md) | incoming | $0.001 |
| **Total** | | **$0.001** |

| Lambda | Memory | Runs / 30 days | Avg duration | Errors / 30 days | Est. $/month (compute + logs) |
|---|---|---|---|---|---|
| `load_rtt_data` | 128 MB | 32 | 10.5 s | 3 | $0.001 |
