# rail-etl datasets

UK rail performance data: journey-level data for the GWR corridor from Newbury through Thatcham (THA)
and Reading (RDG) to London Paddington (PAD), plus ORR's national statistics as a benchmark. See [BRIEF.md](https://github.com/DanteLore/rail-etl/blob/master/BRIEF.md) for the full
source survey and rationale.

## How data flows

```
Realtime Trains API  ──►  incoming.rtt_services          ──►  lake.rtt_service_calls  ──►  lake.rtt_services
HSP API (RDM)        ──►  incoming.hsp_service_calls     ──►  lake.hsp_service_calls  ──►  lake.hsp_services
NR SCHEDULE feed     ──►  incoming.nr_schedule_locations ──►  lake.nr_schedule_days
NR delay attribution ──►  incoming.nr_delay_attribution  ──►  lake.nr_delay_attribution
  (RDM file transfer)       + incoming.nr_da_* (glossary)
ORR Data Portal      ──►  incoming.orr_rail_performance  ──►  lake.orr_operator_* , lake.orr_sub_operator_performance,
                                                              lake.orr_severe_disruption_days, lake.orr_network_rail_performance
```

## incoming — raw landing zone

| Dataset | Table | Description | Cadence |
|---|---|---|---|
| [rtt_services](incoming/rtt_services.md) | `incoming.rtt_services` | RTT actual-vs-scheduled times, cancellations and formation at the nine Newbury–Paddington corridor stations | Daily, 04:00 UTC |
| [rtt_reasons](incoming/rtt_reasons.md) | `incoming.rtt_reasons` | RTT reason codes and text for corridor services with cancelled or terminated calls | Daily, 04:00 UTC |
| [nr_schedule_locations](incoming/nr_schedule_locations.md) | `incoming.nr_schedule_locations` | Network Rail timetable (permanent, overlay, new, cancelled, bus) for corridor trains, one snapshot a day | Daily, 05:00 UTC |
| [nr_delay_attribution](incoming/nr_delay_attribution.md) | `incoming.nr_delay_attribution` | Network Rail's attributed delays and cancellations to every GB passenger train, with cause and responsibility, plus the glossary tables `incoming.nr_da_*` | Checked daily, 05:10 UTC; a new period about every 4 weeks |
| [hsp_service_calls](incoming/hsp_service_calls.md) | `incoming.hsp_service_calls` | HSP scheduled and actual times and reason codes at every stop of Newbury-line trains, with a rolling year of history | Hourly at :30 (yesterday, then filling any gaps) |
| [orr_rail_performance](incoming/orr_rail_performance.md) | `incoming.orr_rail_performance` | The 20 ORR passenger rail performance tables, one row per cell, one snapshot per ORR release | Checked daily, 05:15 UTC; changes about every 4 weeks |

## lake — modelled tables

Rebuilt daily at 05:45 UTC by `model_rail_data`.

| Dataset | Table | Description |
|---|---|---|
| [rtt_service_calls](lake/rtt_service_calls.md) | `lake.rtt_service_calls` | One row per train call at a corridor station: typed times, lateness, on time, cancellation, formation |
| [rtt_services](lake/rtt_services.md) | `lake.rtt_services` | One row per train per day: direction, corridor origin and destination, lateness, cancellations, on time / PPM, short formation |
| [nr_schedule_days](lake/nr_schedule_days.md) | `lake.nr_schedule_days` | One row per corridor train per day: permanent plan vs the plan that applied (cancelled, cut back, overlaid, new, bus) |
| [nr_delay_attribution](lake/nr_delay_attribution.md) | `lake.nr_delay_attribution` | Delays and cancellations to corridor trains with cause, cause group and responsibility (Network Rail, operator, other), from February 2026 |
| [hsp_service_calls](lake/hsp_service_calls.md) | `lake.hsp_service_calls` | One row per stop of every Newbury-line train: full timestamps, lateness, cancellations, reason codes |
| [hsp_services](lake/hsp_services.md) | `lake.hsp_services` | One row per Newbury-line train per day across its whole journey: lateness at Thatcham, Reading and Paddington, cancellations, reason codes |
| [orr_operator_cancellations](lake/orr_operator_cancellations.md) | `lake.orr_operator_cancellations` | ORR 3124: trains planned and cancelled by operator, with cancellation score by responsibility |
| [orr_operator_punctuality](lake/orr_operator_punctuality.md) | `lake.orr_operator_punctuality` | ORR 3138: on time and "Time to N" at station stops by operator |
| [orr_operator_ppm](lake/orr_operator_ppm.md) | `lake.orr_operator_ppm` | ORR 3114a/b: Public Performance Measure by operator and sector |
| [orr_operator_passenger_lateness](lake/orr_operator_passenger_lateness.md) | `lake.orr_operator_passenger_lateness` | ORR 3144a/b: average passenger lateness by operator and sector, from 2010 |
| [orr_operator_delay_causes](lake/orr_operator_delay_causes.md) | `lake.orr_operator_delay_causes` | ORR 3184: delay minutes by operator, responsibility and cause |
| [orr_sub_operator_performance](lake/orr_sub_operator_performance.md) | `lake.orr_sub_operator_performance` | ORR 3167: right time, PPM and CaSL by sub-operator (paused by ORR at 2025-26) |
| [orr_severe_disruption_days](lake/orr_severe_disruption_days.md) | `lake.orr_severe_disruption_days` | ORR 3157a/b: days of severe disruption, GB and by sub-operator |
| [orr_network_rail_performance](lake/orr_network_rail_performance.md) | `lake.orr_network_rail_performance` | ORR 3121/3122, 3131/3132, 3181/3182: cancellations, on time and delay per 1,000 train miles by NR region and route |

**Not built:** Network Rail TRUST and Darwin streaming feeds are deferred.

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
| [hsp_service_calls](incoming/hsp_service_calls.md) | incoming | $0.05 |
| [rtt_reasons](incoming/rtt_reasons.md) | incoming | <$0.001 |
| [nr_schedule_locations](incoming/nr_schedule_locations.md) | incoming | $0.01–0.03 |
| [nr_schedule_days](lake/nr_schedule_days.md) | lake | $0.002 (about $0.09 after a year) |
| [nr_delay_attribution](incoming/nr_delay_attribution.md) | incoming | About $0.01 |
| [nr_delay_attribution](lake/nr_delay_attribution.md) | lake | $0.015 (about $0.04 after a year) |
| [orr_rail_performance](incoming/orr_rail_performance.md) | incoming | $0.01 |
| [rtt_service_calls](lake/rtt_service_calls.md) | lake | $0.003 |
| [rtt_services](lake/rtt_services.md) | lake | $0.002 |
| [hsp_service_calls](lake/hsp_service_calls.md) | lake | $0.006 |
| [hsp_services](lake/hsp_services.md) | lake | $0.002 |
| ORR lake tables (8, see above) | lake | $0.016 |
| **Total** | | **$0.14 now, about $0.27 after a year** |

| Lambda | Memory | Runs / 30 days | Avg duration | Errors / 30 days | Est. $/month (compute + logs) |
|---|---|---|---|---|---|
| `load_rtt_data` | 256 MB (from 2026-11-10) | 32 | 10.5 s (longer from 2026-11-10, with the reason lookups) | 3 | $0.002 |
| `load_hsp_data` | 256 MB | 24 full runs a day during the backfill, then 1 a day plus short hourly checks (new 2026-11-05) | 3 to 8 min per day loaded | 2 on the first day, while the retries were being tuned | $0.05 |
| `load_nr_schedule_data` | 1,024 MB | 30 (expected; new 2026-11-10) | 23 s | 0 | $0.012 |
| `load_nr_delay_attribution_data` | 1,024 MB | 30 (expected; new 2026-11-11) | Under 1 s when there's nothing new; about 25 s per period loaded | 0 | <$0.001 |
| `load_orr_data` | 512 MB | 30 (expected; new 2026-11-05) | 40 s | 0 | $0.010 |
| `model_rail_data` | 128 MB | 30 (expected; new 2026-11-05) | 34 s | 0 | $0.002 |

The 3 `load_rtt_data` errors are one failed run, plus Lambda's two automatic retries, on 2026-10-26. It was loading 2026-10-25, the 25-hour day when the clocks went back. The fix was deployed on 2026-11-05 and the day reloaded (see [rtt_services](incoming/rtt_services.md)).
