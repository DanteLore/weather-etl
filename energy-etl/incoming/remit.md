# incoming.remit

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
| mrid | string | No | | Message ID shared by the revisions of a notification. **Not unique per outage** — some participants reuse it for unrelated outages (see caveats) |
| revision_number | int | No | | Revision of the notification. Usually increments, but some participants repeat or step back numbers (see caveats) |
| publish_time | string | No | | UTC timestamp when this revision was published; use as a polling cursor |
| created_time | string | Yes | | UTC timestamp when the original notification was first created |
| message_type | string | Yes | | `UnavailabilitiesOfElectricityFacilities` (outages) or `OtherMarketInformation` (non-outage notices, no capacity) |
| event_type | string | Yes | | What is unavailable: `Production unavailability`, `Consumption unavailability`, `Transmission unavailability` or `Other unavailability` |
| unavailability_type | string | Yes | | `Planned` or `Unplanned` |
| event_status | string | Yes | | `Active`, `Inactive` (outage over) or `Dismissed` (notification withdrawn) |
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

- **Use [`lake.remit`](../lake/remit.md) for current/final outage state.** This table stores every revision of every notification (820k rows for ~222k outages), so summing capacity here massively overcounts. Taking `MAX(revision_number)` per `mrid` is **not** enough, because (checked 2026-11-05):
  - **The same revision is stored more than once** (12,610 revisions): the 30-minute poll overlaps by 5 minutes, the bulk load overlaps the Lambda, and Elexon sometimes republishes old revisions with a new `publish_time` (but the same `created_time`).
  - **`mrid` is reused** by 3,298 message IDs for unrelated outages, each restarting at revision 1 (e.g. NGET restarting numbering, or placeholder IDs like `0000000000000001-ELXP-RMT-00000001` used for 18 different units).
  - **Revision numbers are unreliable for some participants**, who repeat or step back numbers when updating (e.g. `48W00000THURB-3O-ELXP-RMT-10000056`: 63 updates, mostly re-using revision 2).
  - `lake.remit` handles all three: see its entry for the rule.
- `event_end_time` is null for open-ended outages (the operator does not know when the unit will return). Do not filter `WHERE event_end_time IS NOT NULL` — you will exclude active outages.
- `asset_id` maps to BM unit IDs used in the Elexon tables, but the mapping is not exact — some REMIT assets cover multiple BM units. Use `affected_unit` for human-readable names.
- `outage_profile` is a raw JSON string. Parse it at query time if you need segment-level capacity during complex nuclear outages.
- Partitions are by ingest date, not outage date. An outage starting in January may have revisions partitioned across multiple months.
- Partition columns are unpadded strings.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active. Modelled into [`lake.remit`](../lake/remit.md) (final state per outage) — use that for analysis.
