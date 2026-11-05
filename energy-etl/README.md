# energy-etl

Datasets produced by the [energy-etl](https://github.com/DanteLore/energy-etl) pipeline. Covers UK electricity and gas market data from three sources: the Carbon Intensity API (national and regional carbon intensity), the Elexon BMRS API (wholesale prices, balancing mechanism, generation mix), and commodity price feeds (natural gas SAP, Brent crude oil).

## How data flows

```
Carbon Intensity API  ──►  incoming.carbon_intensity_national
                      ──►  incoming.carbon_intensity_regional
                      ──►  incoming.carbon_intensity_generation

Elexon BMRS API       ──►  incoming.elexon_system_prices  ──►  lake.elexon_system_prices
                      ──►  incoming.elexon_market_index   ──►  lake.elexon_market_index
                      ──►  incoming.elexon_fuelhh         ──►  lake.elexon_fuelhh
                      ──►  incoming.elexon_netbsad        ──►  lake.elexon_netbsad
                      ──►  incoming.elexon_bod            (Parquet)
                      ──►  incoming.elexon_boalf          (Parquet)
                      ──►  incoming.elexon_pn             (Parquet)
                      ──►  incoming.remit

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
| [elexon_fuelhh](lake/elexon_fuelhh.md) | `lake.elexon_fuelhh` | Half-hourly generation by fuel type (MW), one row per settlement period |
| [elexon_system_prices](lake/elexon_system_prices.md) | `lake.elexon_system_prices` | System Buy/Sell Prices and imbalance volume, one row per settlement period |
| [elexon_market_index](lake/elexon_market_index.md) | `lake.elexon_market_index` | Wholesale power price per settlement period, volume-weighted across APX and N2EX |
| [elexon_netbsad](lake/elexon_netbsad.md) | `lake.elexon_netbsad` | Balancing services price adjustments, one row per settlement period |
| [natgas_sap](lake/natgas_sap.md) | `lake.natgas_sap` | Gas System Average Price (p/kWh), one row per gas day |
| [eia_brent_crude](lake/eia_brent_crude.md) | `lake.eia_brent_crude` | Brent crude spot price (USD/barrel), one row per trading day |

## Key things to know

- **Prefer `lake` over `incoming`** wherever a lake table exists.
- **Join Elexon lake tables on `period_start_ts`** (UTC). It is unique per settlement period and avoids clock-change confusion.
- **Elexon settlement periods** run from local (Europe/London) midnight on the settlement date: 48 periods normally, 46 on the spring clock change day and 50 in the autumn. Period 1 starts at 00:00 UTC in winter and 23:00 UTC the previous day in summer.
- **Incoming partitions use unpadded string year/month/day.** Use `DATE_PARSE(year \|\| '-' \|\| month \|\| '-' \|\| day, '%Y-%c-%e')` in Athena — `%c`/`%e` handle single-digit month and day. `MAKE_DATE` is not available in this Presto version.
- **Prefer `elexon_fuelhh` over `carbon_intensity_generation`** for MW-level generation figures; use the carbon intensity tables for intensity index and percentage mix.
- **REMIT records are versioned**: `(mrid, revision_number)` is the primary key. Always take the latest revision per mrid for current outage state.
