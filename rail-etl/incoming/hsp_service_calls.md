# incoming.hsp_service_calls — HSP Historic Service Performance (Newbury line)

## Description

For analysis use [`lake.hsp_service_calls`](../lake/hsp_service_calls.md) and [`lake.hsp_services`](../lake/hsp_services.md).

Scheduled and actual times at every stop of every train that called at both Newbury and Reading, across the train's whole journey (e.g. Bedwyn to Paddington), for each day in about the last year. Each stop also has a delay or cancellation reason code. From National Rail's Historic Service Performance (HSP) API. HSP goes back further than RTT (a rolling year rather than 14 days) and adds reason codes and both the arrival and departure at each stop, but it has no formation data and doesn't cover the Reading–Paddington trains that don't use the Newbury line.

## Classification

Public. Train running data with no personal or customer information.

## Owner

Dan (data platform).

## Source & references

[Rail Data Marketplace](https://raildata.org.uk), product *Historical Service Performance (HSP)*. `POST https://api1.raildata.org.uk/1010-historical-service-performance-_hsp_v1/api/v1/serviceMetrics` lists the RIDs (train IDs) for a pair of stations and a date, and `.../serviceDetails` gives one train's stops. Auth is an `x-apikey` header, stored in Parameter Store as `nationalrailHSPkey` (us-west-2) and bundled in the git-ignored `api_keys.py`. Free: National Rail's [usage charges](https://www.nationalrail.co.uk/developers/darwin-data-feeds/) apply only to the Darwin Webservice feeds.

## License & usage restrictions

Open Government Licence v2.0 with National Rail Enquiries amendments, accepted when subscribing. Attribution to National Rail is required.

## Location

- S3: `s3://dantelore.data.incoming/hsp/service_calls/date=<YYYY-MM-DD>/hsp_service_calls_<YYYY-MM-DD>.parquet`
- Glue: `incoming.hsp_service_calls`
- Format: Parquet, partitioned by `date`

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| rid | string | No | Key | Darwin train ID for this train on this date, e.g. `202610278735157`. Its last five digits matched the digits of RTT's train ID (`C48300`, `W35104`) for all 97 trains on 2026-11-04. This isn't documented by either source |
| date_of_service | string | No | | Date the train started its journey, `YYYY-MM-DD` |
| toc_code | string | No | | Operator code, e.g. `GW` |
| location_order | bigint | No | Key | The stop's position in the train's journey, from 1 |
| location | string | No | | Station CRS code |
| gbtt_pta | string | Yes | | Public (timetable) arrival, `HHMM` UK local time. Null at the origin |
| gbtt_ptd | string | Yes | | Public departure, `HHMM`. Null at the destination |
| actual_ta | string | Yes | | Actual arrival, `HHMM` |
| actual_td | string | Yes | | Actual departure, `HHMM` |
| late_canc_reason | string | Yes | | Numeric Darwin reason code for lateness or cancellation at this stop, e.g. `777`. Applies from the stop where it first appears |
| date | string | No | Partition key | The day this file was loaded for |

## Source ETL / code

`hsp_etl/incoming_lambda/` in the [rail-etl repo](https://github.com/DanteLore/rail-etl), running as the `load_hsp_data` Lambda at 30 minutes past every hour. `hsp_client.py` is the API client, `hsp_transform.py` defines `ROUTES` and the schema, and `lambda_function.py` decides which days to load. Tests are in `tests/test_hsp.py`.

## Freshness & update cadence

Every hour the Lambda lists the days already in S3, then loads yesterday if it's missing (from 04:00 UTC, so normally the 04:30 run), then the oldest missing days in HSP's 364-day window, starting a new day only if at least 10 minutes of the run remain. A day takes 3 to 8 minutes. So it backfills itself at about one day an hour and fills any gap left by a failed run. Once a day is loaded it isn't fetched again.

**History from:** a rolling year. The backfill started on 2026-11-05. It takes about two weeks if each hourly run loads a day, and longer while Lambda is getting 503s. Days older than a year aren't deleted when they leave HSP's window, so history builds up from here.

## Running costs (estimated)

Estimated monthly cost in USD at AWS eu-west-1 list prices, from measurements taken 2026-11-05: one day's file (18 KB, 815 rows) and the first Lambda runs. Excludes tax and the AWS free tier (which currently covers all Lambda usage). Method: [rail-etl README](../README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | About 7 MB a year at 18 KB a day | <$0.001 |
| Lambda `load_hsp_data`, once caught up | One run a day × ~6 min × 256 MB, plus 23 short hourly checks | $0.05 |
| S3 requests (loader) | 720 LISTs, 30 PUTs and Athena result files | $0.004 |
| Athena (loader) | 30 `ADD PARTITION` statements — DDL is free | $0 |
| CloudWatch Logs | Lambda log ingestion | <$0.001 |
| **Total** |  | **$0.05** |

During the backfill, Lambda runs for up to 15 minutes every hour: about 5,400 GB-seconds a day, or about $0.09 a day at list price for about two weeks. That's within the 400,000 GB-seconds the free tier gives each month.

Querying it: a full scan reads about 7 MB per year of data, roughly $0.00005 per query (Athena bills a 10 MB minimum).

## Known issues & caveats

- **Newbury line only.** `serviceMetrics` takes 1 to 2 seconds per train and gives a 504 after about 2 minutes. A whole day of Newbury–Reading (about 40 trains each way) sometimes hit that limit, so each day is queried in seven time windows, which returned exactly the same trains in a test on 2026-10-27. Reading–Paddington (about 250 trains a day each way) timed out as a whole day and isn't loaded. Trains between Newbury and Paddington are included with all their stops; use `lake.rtt_services` for trains that only run Reading–Paddington.
- **The gateway blocks the default `python-requests` User-Agent** with a bare HTML 403, so the client sends its own. A 401 means a bad or missing key.
- **No published rate limit.** Users report 1 request a second, with 429s above that, and the client stays below it. A contributor to the Open Rail Data forum says "HSP was never designed for bulk scraping", which is why the backfill runs hourly.
- **Lambda often gets 503s that a laptop doesn't.** On 2026-11-05 between 22:30 and 23:03 UTC, every `serviceMetrics` call from Lambda returned a bare nginx 503, including on new connections, while the same calls from a laptop worked in 4 to 21 seconds. Lambda had loaded a day earlier that evening, so it's intermittent. The cause isn't confirmed; nginx's rate limiting returns 503 by default, so a limit on AWS's shared addresses is possible.
- **Other short failures.** 429s also occur from both. The client retries each request up to four times over about 2.5 minutes. A run that still fails, or runs out of time, loses only the day in progress and shows in the Lambda error metric.
- **Reason codes are numbers only.** The text is in the Darwin reference data, which isn't loaded yet.
- **HSP has no cancellation flag, and its accuracy is questioned.** Blank actual times "could mean anything - train not reported, train cancelled, or train diverted", and a train missing from HSP may just not have reached it, according to an experienced Open Rail Data contributor ([December 2025 thread](https://groups.google.com/g/openraildata-talk/c/8gV0y9-XDb4)). On 2026-11-04 every stop with no actual times also had a reason code.
- **Agreement with RTT.** On 2026-11-04, joining on train ID, station and scheduled time, 539 of 546 lateness values were within a minute of RTT's. RTT marked 8 stops as cancelled at Aldermaston, Midgham and Theale where HSP has actual times, and HSP marked 1 cancelled where RTT has one. Which is right isn't known.
- Times are `HHMM` with no date, so a train crossing midnight wraps around. The lake works out the full timestamps.

## Interested parties

None yet - new dataset.

## Status

Active.
