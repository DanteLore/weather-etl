# energy-etl

Datasets produced by the [energy-etl](https://github.com/DanteLore/energy-etl) pipeline. Covers UK electricity and gas market data from three sources: the Carbon Intensity API (national and regional carbon intensity), the Elexon BMRS API (wholesale prices, balancing mechanism, generation mix), and commodity price feeds (natural gas SAP, Brent crude oil).

## How data flows

```
Carbon Intensity API  ──►  incoming.carbon_intensity_national    ──►  lake.carbon_intensity_national
                      ──►  incoming.carbon_intensity_regional    ──►  lake.carbon_intensity_regional
                      ──►  incoming.carbon_intensity_generation  ──►  lake.carbon_intensity_generation

Elexon BMRS API       ──►  incoming.elexon_system_prices  ──►  lake.elexon_system_prices
                      ──►  incoming.elexon_market_index   ──►  lake.elexon_market_index
                      ──►  incoming.elexon_fuelhh         ──►  lake.elexon_fuelhh
                      ──►  incoming.elexon_netbsad        ──►  lake.elexon_netbsad
                      ──►  incoming.elexon_bod            (Parquet)
                      ──►  incoming.elexon_boalf          (Parquet)
                      ──►  incoming.elexon_pn             (Parquet)
                      ──►  incoming.remit                ──►  lake.remit

DESNZ / gov.uk        ──►  incoming.repd

National Gas API      ──►  incoming.natgas_sap            ──►  lake.natgas_sap

US EIA API            ──►  incoming.eia_brent_crude       ──►  lake.eia_brent_crude
```

## incoming — raw landing zone

Written directly from source by the per-source loader Lambdas, with no transformation beyond flattening. Use these tables for debugging, auditing or reprocessing. Where a `lake` equivalent exists, use that for analysis: incoming tables can hold duplicates, revisions and string-typed dates.

- S3 bucket: `dantelore.data.incoming`
- Athena database: `incoming`
- Format: NDJSON (JsonSerDe) unless noted as Parquet

| Dataset | Table | Description | Cadence |
|---|---|---|---|
| [carbon_intensity_national](incoming/carbon_intensity_national.md) | `incoming.carbon_intensity_national` | National UK carbon intensity — forecast, actual and index, half-hourly | Every 30 min |
| [carbon_intensity_regional](incoming/carbon_intensity_regional.md) | `incoming.carbon_intensity_regional` | Per-DNO-region carbon intensity and generation mix, half-hourly | Every 30 min |
| [carbon_intensity_generation](incoming/carbon_intensity_generation.md) | `incoming.carbon_intensity_generation` | National fuel-type generation share (%), half-hourly | Every 30 min |
| [elexon_system_prices](incoming/elexon_system_prices.md) | `incoming.elexon_system_prices` | System Buy/Sell Prices and imbalance volume per settlement period | Every 30 min |
| [elexon_market_index](incoming/elexon_market_index.md) | `incoming.elexon_market_index` | Market Index Data (MID) — wholesale prices from N2EX and APX | Every 30 min |
| [elexon_fuelhh](incoming/elexon_fuelhh.md) | `incoming.elexon_fuelhh` | Half-hourly generation by fuel type in MW (FUELHH report) | Every 30 min |
| [elexon_bod](incoming/elexon_bod.md) | `incoming.elexon_bod` | Bid-Offer Data — bid and offer prices/levels per BM unit per period | Daily 06:00 UTC |
| [elexon_boalf](incoming/elexon_boalf.md) | `incoming.elexon_boalf` | Bid-Offer Acceptance Level File — accepted BM instructions | Daily 06:00 UTC |
| [elexon_pn](incoming/elexon_pn.md) | `incoming.elexon_pn` | Physical Notifications — BM unit output plans | Daily 06:00 UTC |
| [elexon_netbsad](incoming/elexon_netbsad.md) | `incoming.elexon_netbsad` | Net Balancing Services Adjustment Data — price adjustments per period | Daily 06:00 UTC |
| [remit](incoming/remit.md) | `incoming.remit` | REMIT outage notifications for generation and transmission assets | Every 30 min |
| [repd](incoming/repd.md) | `incoming.repd` | Renewable Energy Planning Database — all UK renewable/low-carbon projects | Quarterly |
| [natgas_sap](incoming/natgas_sap.md) | `incoming.natgas_sap` | National Gas System Average Price (p/kWh) by gas day | Daily 13:00 UTC |
| [eia_brent_crude](incoming/eia_brent_crude.md) | `incoming.eia_brent_crude` | Europe Brent crude oil daily spot price (USD/barrel) | Daily 20:00 UTC |

## lake — modelled, production-ready

Rebuilt nightly at **07:00 UTC** by the `model_energy_data` Lambda, after the 06:00 Elexon daily loads. Each run deletes and rewrites every table in full from `incoming`, so a fix to the SQL applies to the whole history on the next run. One row per natural key, typed dates and timestamps, stored as Parquet.

- S3 bucket: `dantelore.data.lake`
- Athena database: `lake`
- Format: Parquet, not partitioned (all current tables are small)
- Code: `energy_data_model/sql/<table>.sql`, one file per table

| Dataset | Table | Description |
|---|---|---|
| [carbon_intensity_national](lake/carbon_intensity_national.md) | `lake.carbon_intensity_national` | National carbon intensity (gCO₂/kWh), forecast and actual, one row per half-hour period |
| [carbon_intensity_regional](lake/carbon_intensity_regional.md) | `lake.carbon_intensity_regional` | Regional carbon intensity forecast and generation mix, one row per half-hour period and region |
| [carbon_intensity_generation](lake/carbon_intensity_generation.md) | `lake.carbon_intensity_generation` | National generation mix (%), including embedded wind and solar, one row per half-hour period |
| [elexon_fuelhh](lake/elexon_fuelhh.md) | `lake.elexon_fuelhh` | Half-hourly generation by fuel type (MW), one row per settlement period |
| [elexon_system_prices](lake/elexon_system_prices.md) | `lake.elexon_system_prices` | System Buy/Sell Prices and imbalance volume, one row per settlement period |
| [elexon_market_index](lake/elexon_market_index.md) | `lake.elexon_market_index` | Wholesale power price per settlement period, volume-weighted across APX and N2EX |
| [elexon_netbsad](lake/elexon_netbsad.md) | `lake.elexon_netbsad` | Balancing services price adjustments, one row per settlement period |
| [natgas_sap](lake/natgas_sap.md) | `lake.natgas_sap` | Gas System Average Price (p/kWh), one row per gas day |
| [eia_brent_crude](lake/eia_brent_crude.md) | `lake.eia_brent_crude` | Brent crude spot price (USD/barrel), one row per trading day |
| [remit](lake/remit.md) | `lake.remit` | REMIT outages, final state — one row per outage from its last revision |

## Key things to know

- **Prefer `lake` over `incoming`** wherever a lake table exists.
- **Join Elexon and carbon intensity lake tables on `period_start_ts`** (UTC). It is unique per settlement period and avoids clock-change confusion.
- **Elexon settlement periods** run from local (Europe/London) midnight on the settlement date: 48 periods normally, 46 on the spring clock change day and 50 in the autumn. Period 1 starts at 00:00 UTC in winter and 23:00 UTC the previous day in summer.
- **Incoming partitions use unpadded string year/month/day.** Use `DATE_PARSE(year \|\| '-' \|\| month \|\| '-' \|\| day, '%Y-%c-%e')` in Athena — `%c`/`%e` handle single-digit month and day. `MAKE_DATE` is not available in this Presto version.
- **Prefer `elexon_fuelhh` over `carbon_intensity_generation`** for MW-level generation figures; use the carbon intensity tables for intensity index and percentage mix.
- **Use `lake.remit` for outage state, not `incoming.remit`.** Incoming holds every revision, and `MAX(revision_number)` per `mrid` is not a safe shortcut (mrids are reused, revision numbers repeat). In `lake.remit`, exclude `Dismissed` outages and filter by `event_start_ts`/`event_end_ts` for "current".

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
| [remit](lake/remit.md) | lake | $0.288 |
| [carbon_intensity_regional](incoming/carbon_intensity_regional.md) | incoming | $0.088 |
| [elexon_fuelhh](incoming/elexon_fuelhh.md) | incoming | $0.079 |
| [remit](incoming/remit.md) | incoming | $0.054 |
| [elexon_system_prices](incoming/elexon_system_prices.md) | incoming | $0.051 |
| [elexon_market_index](incoming/elexon_market_index.md) | incoming | $0.051 |
| [elexon_fuelhh](lake/elexon_fuelhh.md) | lake | $0.048 |
| [elexon_netbsad](lake/elexon_netbsad.md) | lake | $0.045 |
| [elexon_system_prices](lake/elexon_system_prices.md) | lake | $0.043 |
| [elexon_market_index](lake/elexon_market_index.md) | lake | $0.041 |
| [elexon_bod](incoming/elexon_bod.md) | incoming | $0.038 |
| [carbon_intensity_generation](incoming/carbon_intensity_generation.md) | incoming | $0.027 |
| [carbon_intensity_national](incoming/carbon_intensity_national.md) | incoming | $0.027 |
| [elexon_pn](incoming/elexon_pn.md) | incoming | $0.024 |
| [elexon_boalf](incoming/elexon_boalf.md) | incoming | $0.008 |
| [eia_brent_crude](lake/eia_brent_crude.md) | lake | $0.003 |
| [natgas_sap](lake/natgas_sap.md) | lake | $0.002 |
| [elexon_netbsad](incoming/elexon_netbsad.md) | incoming | $0.002 |
| [eia_brent_crude](incoming/eia_brent_crude.md) | incoming | $0.002 |
| [natgas_sap](incoming/natgas_sap.md) | incoming | $0.001 |
| [repd](incoming/repd.md) | incoming | $0.001 |
| **Total** | | **$0.92** |

| Lambda | Memory | Runs / 30 days | Avg duration | Errors / 30 days | Est. $/month (compute + logs) |
|---|---|---|---|---|---|
| `load_carbon_intensity_data` | 128 MB | 1,471 | 9.0 s | 30 | $0.029 |
| `load_elexon_wholesale_price_data` | 128 MB | 1,440 | 11.3 s | 0 | $0.036 |
| `load_elexon_fuelhh_data` | 128 MB | 1,440 | 8.4 s | 0 | $0.027 |
| `load_elexon_netbsad_data` | 128 MB | 30 | 4.8 s | 0 | <$0.001 |
| `load_elexon_bod_data` | 512 MB | 31 | 11.9 s | 0 | $0.003 |
| `load_elexon_boalf_data` | 512 MB | 30 | 4.1 s | 0 | $0.001 |
| `load_elexon_pn_data` | 512 MB | 30 | 7.5 s | 0 | $0.002 |
| `load_remit_data` | 128 MB | 1,440 | 4.0 s | 0 | $0.013 |
| `load_natgas_sap_data` | 128 MB | 31 | 6.6 s | 0 | <$0.001 |
| `load_eia_brent_crude_data` | 128 MB | 34 | 15.7 s | 5 | $0.001 |
| `model_energy_data` | 128 MB | 30 (projected) | 42.0 s | — | $0.003 |
