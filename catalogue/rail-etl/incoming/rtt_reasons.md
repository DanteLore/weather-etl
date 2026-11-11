# incoming.rtt_reasons — RTT Cancellation Reasons

## Description

The reason code and text RTT gives for each corridor service with a cancelled, terminated-short or started-short call, e.g. `TB` "the train operator's request" or `IB` "a points failure". Codes are Network Rail's two-letter TRUST delay attribution codes. Joined into [`lake.rtt_services`](../lake/rtt_services.md) as `cancellation_reason_text`.

## Classification

Public. Train running data with no personal or customer information.

## Owner

Dan (data platform).

## Source & references

Realtime Trains API, `GET /gb-nr/service` (its `reasons` block). See [`incoming.rtt_services`](rtt_services.md) for the API and licence.

## License & usage restrictions

Realtime Trains free tier: personal and non-commercial use only.

## Location

- S3: `s3://dantelore.data.incoming/rtt/reasons/date=<YYYY-MM-DD>/rtt_reasons_<YYYY-MM-DD>.parquet`
- Glue: `incoming.rtt_reasons`
- Format: Parquet, partitioned by `date` (the same load date as `incoming.rtt_services`)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| unique_identity | string | No | Key | RTT service identity, as in `incoming.rtt_services` |
| departure_date | string | No | | Date the service left its origin |
| reason_type | string | Yes | Key | `CANCEL` (the only type seen so far) |
| reason_code | string | Yes | Key | TRUST delay attribution code, e.g. `ZW`, `TB`, `IB` |
| reason_system | string | Yes | | `TRUST` |
| short_text | string | Yes | | Short description, e.g. `train operator's request` |
| long_text | string | Yes | | Longer description, e.g. `the train operator's request` |
| date | string | No | Partition key | Load date |

## Source ETL / code

`fetch_reasons` in `rtt_etl/incoming_lambda/rtt_transform.py`, called by the `load_rtt_data` Lambda and `rtt_etl/rtt_bulk_load.py` after each day's services are loaded. It makes one service call per disrupted service, about 5 to 90 a day.

## Freshness & update cadence

Daily with `incoming.rtt_services`, at 04:00 UTC.

**History from:** 2026-10-28, the earliest date RTT still had when this was added on 2026-11-10.

## Running costs (estimated)

Estimated monthly cost in USD at AWS eu-west-1 list prices, from measurements taken 2026-11-11: 13 days of files (34 KB in total). Excludes tax and the AWS free tier (which currently covers all Lambda usage). Method: [rail-etl README](../README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | About 3 KB a day | <$0.001 |
| Lambda | Part of `load_rtt_data` (see `incoming.rtt_services`) | — |
| S3 writes | 30 files | <$0.001 |
| **Total** |  | **<$0.001** |

## Known issues & caveats

- **Responsibility groups aren't loaded yet.** The codes are grouped by who caused the delay (e.g. X for external events, Y for reactionary delay, per ORR's published amendments to the Delay Attribution Principles and Rules), but the full current code list (DAPR Section S) isn't available to automated downloads. Network Rail's site blocks them.
- `ZW` ("unknown cause") is the most common code, used where the cause hasn't been attributed yet.

## Interested parties

The Thatcham rail page analysis.

## Status

Active.
