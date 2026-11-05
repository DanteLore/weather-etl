# lake.elexon_fuelhh — Half-hourly Generation by Fuel Type

## Description

Metered GB generation by fuel type in MW for every half-hour settlement period, from Elexon's FUELHH report: gas (CCGT/OCGT), coal, nuclear, wind, hydro, pumped storage, biomass and each interconnector. One row per settlement period with typed dates and timestamps. The cleanest table for generation mix in absolute MW.

## Classification

Public. Derived from public open data; no personal data.

## Owner

Dan. See the [energy-etl repo](https://github.com/DanteLore/energy-etl) for the code.

## Source & references

Derived from [`incoming.elexon_fuelhh`](../incoming/elexon_fuelhh.md). Original source: Elexon BMRS FUELHH.

## License & usage restrictions

Elexon open data licence. Free to use; attribution to Elexon preferred. No restrictions on internal use.

## Location

- S3: `s3://dantelore.data.lake/elexon_fuelhh/`
- Glue: `lake.elexon_fuelhh`
- Format: Parquet (ParquetHiveSerDe)
- Not partitioned (small table — a full scan is a few MB)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| settlement_date | date | No | Primary (with settlement_period) | Settlement date (UK local calendar day) |
| settlement_period | int | No | Primary (with settlement_date) | Half-hour period 1–48 from local (Europe/London) midnight; 46 on the spring clock change day, 50 in autumn |
| period_start_ts | timestamp | No | Unique; join key across Elexon lake tables | UTC start of the settlement period. Use this to join Elexon lake tables to each other |
| publish_ts | timestamp | No |  | UTC time Elexon published the figures for this period |
| gen_biomass | int | Yes |  | Biomass generation (MW) |
| gen_ccgt | int | Yes |  | Combined Cycle Gas Turbine generation (MW) |
| gen_coal | int | Yes |  | Coal generation (MW) |
| gen_nuclear | int | Yes |  | Nuclear generation (MW) |
| gen_ocgt | int | Yes |  | Open Cycle Gas Turbine generation (MW) |
| gen_oil | int | Yes |  | Oil generation (MW) |
| gen_other | int | Yes |  | Other generation (MW) |
| gen_npshyd | int | Yes |  | Non-pumped-storage hydro generation (MW) |
| gen_ps | int | Yes |  | Pumped storage (MW; negative = charging from grid) |
| gen_wind | int | Yes |  | Wind generation (MW) |
| gen_intelec | int | Yes |  | ElecLink interconnector to France (MW; negative = export) |
| gen_intew | int | Yes |  | East-West interconnector to Ireland (MW; negative = export) |
| gen_intfr | int | Yes |  | IFA interconnector to France (MW; negative = export) |
| gen_intgrnl | int | Yes |  | Greenlink interconnector to Ireland (MW; negative = export) |
| gen_intifa2 | int | Yes |  | IFA2 interconnector to France (MW; negative = export) |
| gen_intirl | int | Yes |  | Moyle interconnector to Northern Ireland (MW; negative = export) |
| gen_intned | int | Yes |  | BritNed interconnector to Netherlands (MW; negative = export) |
| gen_intnem | int | Yes |  | NEMO interconnector to Belgium (MW; negative = export) |
| gen_intnsl | int | Yes |  | North Sea Link interconnector to Norway (MW; negative = export) |
| gen_intvkl | int | Yes |  | Viking interconnector to Denmark (MW; negative = export) |

## Source ETL / code

[`github.com/DanteLore/energy-etl`](https://github.com/DanteLore/energy-etl), `energy_data_model/sql/elexon_fuelhh.sql`. Lambda function `model_energy_data`.

SQL summary: one row per `(settlement_date, settlement_period)`, keeping the most recently published row if the source ever holds more than one; strings cast to `date`/`timestamp`.

## Freshness & update cadence

Full rebuild every night at 07:00 UTC by `model_energy_data`: the S3 folder is deleted and the whole table rewritten from `incoming.elexon_fuelhh`. Incoming is updated every 30 minutes, so the lake lags by up to a day. The rebuild takes about 30 seconds for all energy lake tables; queries at 07:00 UTC may briefly see an empty table.

## Running costs (estimated)

Estimated monthly cost in USD at AWS eu-west-1 list prices, from measurements taken 2026-11-05: the S3 size of this table's location, plus the last 30 days of Lambda metrics and Athena query history. Excludes tax and the AWS free tier (which currently covers all Lambda usage). Method: [energy-etl README](../README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.006 GB, 6 objects | <$0.001 |
| Lambda `model_energy_data` (9% share) | 30 rebuilds/month | <$0.001 |
| Athena (rebuild) | 30 × 120 MB scanned of `incoming.elexon_fuelhh` | $0.018 |
| S3 reads (rebuild) | 30 × 2,501 objects | $0.030 |
| CloudWatch Logs | Lambda log ingestion | $0 |
| **Total** | | **$0.048** |

Querying it: a full scan reads about 0.01 GB, roughly <$0.001 per query (Athena bills a 10 MB minimum).

## Known issues & caveats

- Transmission-metered generation only: no embedded (rooftop solar, small wind) generation, so wind and solar are lower than the Carbon Intensity API's percentages.
- `gen_ps` is negative when pumped storage is charging; interconnector columns are negative on export.
- Interconnector columns are null before the link existed (e.g. `gen_intvkl` Viking, `gen_intgrnl` Greenlink, `gen_intnsl` North Sea Link). Null means "not yet operating", not zero.
- There is no solar column: FUELHH does not report solar.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
