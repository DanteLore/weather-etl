# remit

## Description

REMIT (Regulation on Energy Market Integrity and Transparency) outage notifications for UK generation and transmission assets, sourced from Elexon's BMRS. Each row is one revision of one outage notification, covering planned and unplanned unavailability for power stations, interconnectors, and other assets. Records are versioned — the same outage may appear multiple times as the operator updates it. Use this to track capacity unavailability over time or to understand supply-side risk in a given period.

## Classification

Public. REMIT notifications are mandated to be publicly disclosed. Elexon acts as a transparency platform.

## Owner

Dan (energy-etl). Query via `incoming.remit` in Athena.

## Source & references

- Elexon BMRS API: https://developer.data.elexon.co.uk/
- REMIT transparency obligations under EU Regulation 1227/2011, retained in UK law post-Brexit.
- API key required (stored in `api_keys.py`, not in this repo).

## License & usage restrictions

Public disclosure data. No restrictions on use.

## Location

- S3: `s3://dantelore.data.incoming/remit/`
- Glue / Athena: `incoming.remit`
- Format: NDJSON (JsonSerDe), partitioned by year/month/day (by ingest date)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| mrid | string | No | Primary (with revision_number) | Stable message group ID — same across all revisions of one outage notification |
| revision_number | int | No | Primary (with mrid) | Increments on each update; take max per mrid for current state |
| publish_time | string | No | | UTC timestamp when this revision was published; use as a polling cursor |
| created_time | string | Yes | | UTC timestamp when the original notification was first created |
| message_type | string | Yes | | REMIT message type (e.g. `UnavailabilityOfGenerationUnit`) |
| event_type | string | Yes | | Planned or unplanned |
| unavailability_type | string | Yes | | Production unavailability, consumption unavailability, etc. |
| event_status | string | Yes | | Active, Cancelled, etc. |
| participant_id | string | Yes | | Market participant EIC code |
| asset_id | string | Yes | | Asset EIC code — maps to BM unit ID in `elexon_bod`/`elexon_boalf` |
| asset_type | string | Yes | | Generation unit, interconnector, etc. |
| affected_unit | string | Yes | | Name of the affected unit |
| fuel_type | string | Yes | | Fuel type of the asset (e.g. `Nuclear`, `Wind`) |
| normal_capacity | double | Yes | | Registered capacity in MW; may be null for demand-side assets — do not divide by this without checking |
| available_capacity | double | Yes | | Available capacity during the outage (MW) |
| unavailable_capacity | double | Yes | | Capacity unavailable during the outage (MW) |
| event_start_time | string | Yes | | UTC start of the outage |
| event_end_time | string | Yes | | UTC end of the outage; null means open-ended — do not filter these out |
| duration_uncertainty | string | Yes | | Operator's indication of certainty about the end time |
| cause | string | Yes | | Free-text reason for the outage |
| related_information | string | Yes | | Additional notes from the operator |
| outage_profile | string | Yes | | JSON-encoded capacity profile segments (present on some nuclear units with partial outages) |
| year | string | No | Partition | Unpadded year of ingest date |
| month | string | No | Partition | Unpadded month of ingest date |
| day | string | No | Partition | Unpadded day of ingest date |

## Source ETL / code

`energy-etl` repo, `remit_etl/`. Lambda runs every 30 minutes, fetching recently published notifications.

## Freshness & update cadence

Updated every 30 minutes. REMIT notifications can be published at any time; operators are required to publish as soon as technically possible after becoming aware of an outage.

**History from:** 2017-06-28 (when REMIT reporting began in GB).

## Known issues & caveats

- **This table is append-only versioned.** Every update to an outage appends a new row with a higher `revision_number`. To get current outage state, always filter to `MAX(revision_number)` per `mrid`. Summing unavailable capacity without this deduplication will massively overcount.
- `event_end_time` is null for open-ended outages (the operator does not know when the unit will return). Do not filter `WHERE event_end_time IS NOT NULL` — you will exclude active outages.
- `asset_id` maps to BM unit IDs used in the Elexon tables, but the mapping is not exact — some REMIT assets cover multiple BM units. Use `affected_unit` for human-readable names.
- `outage_profile` is a raw JSON string. Parse it at query time if you need segment-level capacity during complex nuclear outages.
- Partitions are by ingest date, not outage date. An outage starting in January may have revisions partitioned across multiple months.
- Partition columns are unpadded strings.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
