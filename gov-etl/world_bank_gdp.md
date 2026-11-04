# World Bank GDP Indicators

## Description

GDP indicators for all countries and territories, sourced from the World Bank
API. Four indicators are loaded: GDP (current US$), GDP (constant 2015 US$),
GDP at purchasing power parity (current international $), and GDP per capita
(current US$). Data covers 1960 onwards; coverage varies by country and
indicator. Useful for macroeconomic comparisons, normalising other datasets
by country size, and long-run economic time-series analysis.

## Classification

Public. Official World Bank statistics republished via a free open API. No
personal data.

## Owner

Dan. See gov-etl repo for the code.

## Source & references

World Bank API: https://api.worldbank.org/v2

API documentation: https://datahelpdesk.worldbank.org/knowledgebase/articles/889392-about-the-indicators-api-documentation

Indicator definitions:
- `NY.GDP.MKTP.CD` — GDP (current US$)
- `NY.GDP.MKTP.KD` — GDP (constant 2015 US$)
- `NY.GDP.MKTP.PP.CD` — GDP, PPP (current international $)
- `NY.GDP.PCAP.CD` — GDP per capita (current US$)

## License & usage restrictions

Creative Commons Attribution 4.0 International (CC-BY 4.0) — the World Bank's
default license for datasets it produces itself. Free to copy, modify and
distribute for any purpose, including commercial use, provided appropriate
credit is given and any changes are indicated. Some World Bank datasets carry
other licenses (e.g. ODbL, Microdata Research License) — this applies to the
GDP indicators specifically, not the World Bank catalogue as a whole.

## Location

- S3: `s3://dantelore.data.incoming/world_bank_gdp/world_bank_gdp.parquet`
- Glue: `incoming.world_bank_gdp`
- Format: Parquet, single file (not partitioned)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| country_code | string | No | Primary (with indicator_id, year) | ISO 3-letter country/region code |
| country_name | string | No | | Country or region name |
| indicator_id | string | No | Primary (with country_code, year) | World Bank indicator code, e.g. `NY.GDP.MKTP.CD` |
| indicator_name | string | No | | Human-readable indicator name |
| year | int | No | Primary (with country_code, indicator_id) | Calendar year the value relates to |
| value | double | Yes | | Indicator value; null where the World Bank has no data for that country/indicator/year |

## Source ETL / code

`github.com/dantelore/gov-etl`, `world_bank_gdp/world_bank_gdp_bulk_load.py`.
Run manually — no arguments:

```
python world_bank_gdp/world_bank_gdp_bulk_load.py
```

## Freshness & update cadence

Manually triggered, full reload only — each run downloads all countries and
years for all four indicators and replaces the single output file entirely.
There is no incremental/partitioned refresh (unlike `exchange_rates`). Re-run
periodically to pick up revised or newly published figures.

## Data summary (as loaded)

| Metric | Value |
|---|---|
| Total rows | 140,184 |
| Countries/regions | 262 |
| Years covered | 1960–2025 |

| Indicator | Rows | Non-null | Years |
|---|---|---|---|
| NY.GDP.MKTP.CD (GDP current US$) | 35,046 | 29,306 | 1960–2025 |
| NY.GDP.MKTP.KD (GDP constant 2015 US$) | 35,046 | 28,801 | 1960–2025 |
| NY.GDP.MKTP.PP.CD (GDP PPP current int'l $) | 35,046 | 17,223 | 1960–2025 |
| NY.GDP.PCAP.CD (GDP per capita current US$) | 35,046 | 29,306 | 1960–2025 |

## Known issues & caveats

- `value` is frequently null — coverage varies significantly by country and
  indicator, especially for earlier years and smaller/newer states. PPP
  (`NY.GDP.MKTP.PP.CD`) is by far the sparsest at ~49% non-null vs ~84% for
  the other three indicators, since PPP conversion factors aren't published
  for every country/year.
- The World Bank revises historical figures over time (e.g. GDP restatements,
  methodology changes); since this is a full reload each run, previously
  loaded values can change or disappear on a re-run rather than being
  versioned.
- `country_code`/`country_name` includes World Bank aggregate regions (e.g.
  "World", "European Union", income-group aggregates) alongside individual
  countries — filter on known ISO country codes if aggregates would skew
  analysis. This is why "countries/regions" above (262) is higher than the
  ~217 actual sovereign states/territories with World Bank data.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
