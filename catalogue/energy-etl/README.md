# energy-etl

Datasets produced by the [energy-etl](https://github.com/DanteLore/energy-etl) pipeline. Covers UK electricity and gas market data from three sources: the Carbon Intensity API (national and regional carbon intensity), the Elexon BMRS API (wholesale prices, balancing mechanism, generation mix), and commodity price feeds (natural gas SAP, Brent crude oil).

All datasets land in the `incoming` Athena database backed by `s3://dantelore.data.incoming/`. All tables are NDJSON (JsonSerDe) unless noted as Parquet.

## How data flows

```
Carbon Intensity API  ──►  incoming.carbon_intensity_national
                      ──►  incoming.carbon_intensity_regional
                      ──►  incoming.carbon_intensity_generation

Elexon BMRS API       ──►  incoming.elexon_system_prices
                      ──►  incoming.elexon_market_index
                      ──►  incoming.elexon_fuelhh
                      ──►  incoming.elexon_bod          (Parquet)
                      ──►  incoming.elexon_boalf        (Parquet)
                      ──►  incoming.elexon_pn           (Parquet)
                      ──►  incoming.elexon_netbsad

Elexon BMRS API       ──►  incoming.remit

DESNZ / gov.uk        ──►  incoming.repd

National Gas API      ──►  incoming.natgas_sap

US EIA API            ──►  incoming.eia_brent_crude
```

## Datasets

| Dataset | Table | Description | Cadence |
|---|---|---|---|
| [carbon_intensity_national](carbon_intensity_national.md) | `incoming.carbon_intensity_national` | National UK carbon intensity — forecast, actual and index, half-hourly | Every 30 min |
| [carbon_intensity_regional](carbon_intensity_regional.md) | `incoming.carbon_intensity_regional` | Per-DNO-region carbon intensity and generation mix, half-hourly | Every 30 min |
| [carbon_intensity_generation](carbon_intensity_generation.md) | `incoming.carbon_intensity_generation` | National fuel-type generation share (%), half-hourly | Every 30 min |
| [elexon_system_prices](elexon_system_prices.md) | `incoming.elexon_system_prices` | System Buy/Sell Prices and imbalance volume per settlement period | Every 30 min |
| [elexon_market_index](elexon_market_index.md) | `incoming.elexon_market_index` | Market Index Data (MID) — wholesale prices from N2EX and APX | Every 30 min |
| [elexon_fuelhh](elexon_fuelhh.md) | `incoming.elexon_fuelhh` | Half-hourly generation by fuel type in MW (FUELHH report) | Every 30 min |
| [elexon_bod](elexon_bod.md) | `incoming.elexon_bod` | Bid-Offer Data — bid and offer prices/levels per BM unit per period | Daily 06:00 UTC |
| [elexon_boalf](elexon_boalf.md) | `incoming.elexon_boalf` | Bid-Offer Acceptance Level File — accepted BM instructions | Daily 06:00 UTC |
| [elexon_pn](elexon_pn.md) | `incoming.elexon_pn` | Physical Notifications — BM unit output plans | Daily 06:00 UTC |
| [elexon_netbsad](elexon_netbsad.md) | `incoming.elexon_netbsad` | Net Balancing Services Adjustment Data — price adjustments per period | Daily 06:00 UTC |
| [remit](remit.md) | `incoming.remit` | REMIT outage notifications for generation and transmission assets | Every 30 min |
| [repd](repd.md) | `incoming.repd` | Renewable Energy Planning Database — all UK renewable/low-carbon projects | Quarterly |
| [natgas_sap](natgas_sap.md) | `incoming.natgas_sap` | National Gas System Average Price (p/kWh) by gas day | Daily 13:00 UTC |
| [eia_brent_crude](eia_brent_crude.md) | `incoming.eia_brent_crude` | Europe Brent crude oil daily spot price (USD/barrel) | Daily 20:00 UTC |

## Key things to know

- **All partitions use unpadded string year/month/day.** Use `DATE_PARSE(year \|\| '-' \|\| month \|\| '-' \|\| day, '%Y-%c-%e')` in Athena — `%c`/`%e` handle single-digit month and day. `MAKE_DATE` is not available in this Presto version.
- **Elexon settlement periods** run 1–48 (half-hours from midnight). Period 1 = 00:00–00:30 UTC on the settlement date.
- **Prefer `elexon_fuelhh` over `carbon_intensity_generation`** for MW-level generation figures; use the carbon intensity tables for intensity index and percentage mix.
- **REMIT records are versioned**: `(mrid, revision_number)` is the primary key. Always take the latest revision per mrid for current outage state.
