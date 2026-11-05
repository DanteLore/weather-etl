# lake.remit — REMIT Outages, Final State

## Description

The final state of every REMIT outage notification for GB generation, demand and transmission assets: power station outages (planned and unplanned), closures, interconnector and transmission unavailability. One row per outage, taken from the last revision the market participant published. Use this to answer "what capacity is (or was) unavailable, when, and why" — e.g. current unavailable generation by fuel type, or the history of nuclear outages.

Derived from [`incoming.remit`](../incoming/remit.md), which holds every revision (820k rows for ~222k outages).

## Classification

Public. REMIT notifications are mandated to be publicly disclosed. Elexon acts as a transparency platform.

## Owner

Dan. See the [energy-etl repo](https://github.com/DanteLore/energy-etl) for the code.

## Source & references

Derived from [`incoming.remit`](../incoming/remit.md). Original source: Elexon BMRS REMIT API; REMIT transparency obligations under EU Regulation 1227/2011, retained in UK law.

## License & usage restrictions

Public disclosure data. No restrictions on use.

## Location

- S3: `s3://dantelore.data.lake/remit/`
- Glue: `lake.remit`
- Format: Parquet (ParquetHiveSerDe)
- Not partitioned (~222k rows; a full scan is under 10 MB)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| mrid | string | No | Primary (with outage_created_ts) | Message ID. Not unique on its own — some participants reuse IDs for unrelated outages |
| outage_created_ts | timestamp | No | Primary (with mrid) | UTC creation time of the outage's first revision. Together with `mrid`, identifies one outage |
| revision_number | int | No | | Revision number of the final revision (as reported; not always the highest number seen) |
| revisions_seen | int | No | | Number of distinct revisions of this outage in incoming |
| revision_created_ts | timestamp | No | | UTC time the final revision was created by the participant |
| revision_published_ts | timestamp | No | | UTC time the final revision was (last) published by Elexon |
| message_type | string | No | | `UnavailabilitiesOfElectricityFacilities` (outages) or `OtherMarketInformation` (non-outage notices, no capacity) |
| event_type | string | Yes | | What is unavailable: `Production unavailability`, `Consumption unavailability`, `Transmission unavailability` or `Other unavailability` |
| unavailability_type | string | Yes | | `Planned` or `Unplanned` |
| event_status | string | No | | Final status: `Active`, `Inactive` (outage over) or `Dismissed` (notification withdrawn — the outage did not happen as notified) |
| participant_id | string | Yes | | Market participant EIC code |
| asset_id | string | Yes | | Asset EIC code — maps approximately to BM unit IDs in the Elexon tables |
| asset_type | string | Yes | | Generation unit, interconnector, etc. |
| affected_unit | string | Yes | | Name of the affected unit (e.g. `T_THURB-3`, `RATS-1`) |
| fuel_type | string | Yes | | Fuel type of the asset (e.g. `Nuclear`, `Fossil Gas`, `Wind Offshore`). Null for transmission assets |
| normal_capacity | double | Yes | | Registered capacity in MW; may be null — check before dividing by it |
| available_capacity | double | Yes | | Available capacity during the outage (MW) |
| unavailable_capacity | double | Yes | | Capacity unavailable during the outage (MW). Null for `OtherMarketInformation` |
| event_start_ts | timestamp | No | | UTC start of the outage |
| event_end_ts | timestamp | Yes | | UTC end of the outage; null means open-ended — do not filter these out |
| duration_uncertainty | string | Yes | | Operator's indication of certainty about the end time |
| cause | string | Yes | | Free-text reason for the outage |
| related_information | string | Yes | | Additional notes from the operator |
| outage_profile | string | Yes | | JSON-encoded capacity profile segments (some nuclear units with partial outages). Parse at query time |

## Source ETL / code

[`github.com/DanteLore/energy-etl`](https://github.com/DanteLore/energy-etl), `energy_data_model/sql/remit.sql`. Lambda function `model_energy_data`.

The rule, worked out from the data on 2026-11-05 (see the incoming entry for the evidence):

1. **Collapse copies:** one row per `(mrid, revision_number, created_time)`, keeping the latest publication. This removes poll-overlap duplicates and Elexon republishes.
2. **Split reused mrids into outages:** within each `mrid`, in `created_time` order, a new outage starts at every revision 1. Revisions before the first revision 1 (history that predates our data) form their own outage.
3. **Final state = most recently created revision** of each outage (ties: higher revision number, then later publish time). Not the highest revision number, because some participants repeat or step back revision numbers.

Spot-checked: `48W00000THURB-3O-ELXP-RMT-10000056` → 1 outage (63 updates, final Inactive); `48X000000000003E-NGET-RMT-00001403` → 2 outages on different units (SVRP-10, Nov 2025; SVRP-20, 2026); `48X000000000283J-NGET-RMT-00034488` → 1 outage, final revision 4 Inactive despite all revisions being republished later.

## Freshness & update cadence

Full rebuild every night at 07:00 UTC by `model_energy_data`: the S3 folder is deleted and the whole table rewritten from `incoming.remit` (~1 GB scanned, ~11 seconds). Incoming is polled every 30 minutes, so the lake lags by up to a day. Queries at 07:00 UTC may briefly see an empty table.

## Running costs (estimated)

Estimated monthly cost in USD at AWS eu-west-1 list prices, from measurements taken 2026-11-05: the S3 size of this table's location, plus the last 30 days of Lambda metrics and Athena query history. Excludes tax and the AWS free tier (which currently covers all Lambda usage). Method: [energy-etl README](../README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.017 GB, 21 objects | <$0.001 |
| Lambda `model_energy_data` (72% share) | 30 rebuilds/month | $0.002 |
| Athena (rebuild) | 30 × 978 MB scanned of `incoming.remit` | $0.147 |
| S3 reads (rebuild) | 30 × 11,564 objects | $0.139 |
| CloudWatch Logs | Lambda log ingestion | $0 |
| **Total** | | **$0.29** |

Querying it: a full scan reads about 0.02 GB, roughly <$0.001 per query (Athena bills a 10 MB minimum).

## Known issues & caveats

- **Exclude `Dismissed` when counting outages.** 88,419 of 222,050 outages (40%) end Dismissed — withdrawn notifications. Final states: 68,338 Active, 65,293 Inactive, 88,419 Dismissed.
- **"Active" does not mean "happening now".** For current unavailability, filter `event_status = 'Active' AND event_start_ts <= now AND (event_end_ts IS NULL OR event_end_ts > now)`.
- **Closures are reported as very long outages.** Closed coal stations (Ratcliffe `RATS-1..4`, Fiddler's Ferry, Ferrybridge, Drax coal units) appear as Active outages ending 2035–2050. 23 currently-active outages end after 2030. Exclude these when measuring temporary unavailability, or you will see ~5.6 GW of "unavailable coal" in a system with no coal plant.
- **Filter on `event_type = 'Production unavailability'`** for generation; transmission outages report line/circuit capacity, which is not generation and should not be summed with it.
- **`OtherMarketInformation` rows are not outages** (1,630 rows, no capacity). Filter on `message_type = 'UnavailabilitiesOfElectricityFacilities'` for outage analysis.
- **Bad dates are kept as reported:** 11 outages have impossible start dates (`0001-01-01`, `1970-01-01`, and `1020-…`, probably a typo for 2020) and 7 end before they start.
- **The outage split is a heuristic.** If a participant reused an mrid for a new outage without restarting at revision 1, the two are merged; if they issued a second revision 1 for the same outage, it is split. The spot checks above behaved correctly, but treat `(mrid, outage_created_ts)` as a best-effort outage ID.
- 103 outages are open-ended (`event_end_ts` null).
- `asset_id` maps to BM unit IDs in the Elexon tables, but not exactly — some REMIT assets cover several BM units.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
