# rail-etl datasets

UK rail performance data: journey-level data for the GWR corridor from Newbury through Thatcham (THA)
and Reading (RDG) to London Paddington (PAD), plus ORR's national statistics as a benchmark. See [BRIEF.md](https://github.com/DanteLore/rail-etl/blob/master/BRIEF.md) for the full
source survey and rationale.

## How data flows

```
Realtime Trains API  ──►  incoming.rtt_services          ──►  lake.rtt_service_calls  ──►  lake.rtt_services
ORR Data Portal      ──►  incoming.orr_rail_performance  ──►  lake.orr_rail_performance
```

## incoming — raw landing zone

| Dataset | Table | Description | Cadence |
|---|---|---|---|
| [rtt_services](incoming/rtt_services.md) | `incoming.rtt_services` | RTT actual-vs-scheduled times, cancellations and formation at the nine Newbury–Paddington corridor stations | Daily, 04:00 UTC |
| [orr_rail_performance](incoming/orr_rail_performance.md) | `incoming.orr_rail_performance` | The 20 ORR passenger rail performance tables, one row per cell, one snapshot per ORR release | Checked daily, 05:15 UTC; changes about every 4 weeks |

## lake — modelled tables

Rebuilt daily at 05:45 UTC by `model_rail_data`.

| Dataset | Table | Description |
|---|---|---|
| [rtt_service_calls](lake/rtt_service_calls.md) | `lake.rtt_service_calls` | One row per train call at a corridor station: typed times, lateness, on time, cancellation, formation |
| [rtt_services](lake/rtt_services.md) | `lake.rtt_services` | One row per train per day: direction, corridor origin and destination, lateness, cancellations, on time / PPM, short formation |
| [orr_rail_performance](lake/orr_rail_performance.md) | `lake.orr_rail_performance` | National benchmark: every ORR performance number by period and operator, sector, region or route |

**Not built yet:** HSP (Historic Service Performance), which needs a free Rail Data Marketplace account. Network Rail TRUST and Darwin streaming feeds are deferred.

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
| [rtt_services](incoming/rtt_services.md) | incoming | $0.001 |
| [orr_rail_performance](incoming/orr_rail_performance.md) | incoming | $0.01 |
| [rtt_service_calls](lake/rtt_service_calls.md) | lake | $0.003 |
| [rtt_services](lake/rtt_services.md) | lake | $0.002 |
| [orr_rail_performance](lake/orr_rail_performance.md) | lake | $0.002 |
| **Total** | | **$0.02** |

| Lambda | Memory | Runs / 30 days | Avg duration | Errors / 30 days | Est. $/month (compute + logs) |
|---|---|---|---|---|---|
| `load_rtt_data` | 128 MB | 32 | 10.5 s | 3 | $0.001 |
| `load_orr_data` | 512 MB | 30 (expected; new 2026-11-05) | 40 s | 0 | $0.010 |
| `model_rail_data` | 128 MB | 30 (expected; new 2026-11-05) | 15 s | 0 | $0.001 |

The 3 `load_rtt_data` errors are one failed run, plus Lambda's two automatic retries, on 2026-10-26. It was loading 2026-10-25, the 25-hour day when the clocks went back. The fix was deployed on 2026-11-05 and the day reloaded (see [rtt_services](incoming/rtt_services.md)).
