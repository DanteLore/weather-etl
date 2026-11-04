# Exchange Rates (Frankfurter / ECB)

## Description

Daily foreign exchange reference rates for 46 currencies, sourced from the
European Central Bank (ECB) via the Frankfurter API. All rates are expressed
as units of the target currency per 1 EUR (EUR-based). Data covers every ECB
trading day from 1999-01-04 onwards. Useful for currency conversion, price
normalisation across countries, and time-series analysis of FX movements.

## Classification

Public. Official central bank reference rates republished via a free open API.
No personal data.

## Owner

Dan. See gov-etl repo for the code.

## Source & references

Frankfurter API (ECB reference rates): https://frankfurter.dev

API documentation: https://frankfurter.dev/v1/

ECB Euro reference rates: https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/

## License & usage restrictions

ECB reference rates are published as open data. Frankfurter is a free, open-source
API with no usage restrictions beyond fair-use rate limiting. No attribution
required, but Frankfurter should be cited as the API source.

## Location

- S3: `s3://dantelore.data.incoming/exchange_rates/frankfurter/year=<YYYY>/`
- Glue: `incoming.exchange_rates_frankfurter`
- Format: Parquet, partitioned by `year`

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| date | string | No | Primary (with currency) | Trading date in ISO 8601 format (YYYY-MM-DD) |
| base_currency | string | No | | Base currency for all rates — always EUR |
| currency | string | No | Primary (with date) | ISO 4217 currency code (e.g. GBP, USD, JPY) |
| rate | double | Yes | | Units of `currency` per 1 EUR on this date |

## Source ETL / code

`github.com/dantelore/gov-etl`, `exchange_rates/exchange_rates_bulk_load.py`.
Run manually to bulk-load one or more years:

```
python exchange_rates/exchange_rates_bulk_load.py --start 1999
python exchange_rates/exchange_rates_bulk_load.py --start 2024 --end 2024
```

## Freshness & update cadence

Bulk-loaded by year, manually triggered. ECB publishes rates on each TARGET2
trading day (weekdays, excluding ECB holidays). There is no automatic refresh —
run the script to pick up a new year or re-run the current year to get recent dates.

## Data summary (as loaded)

| Metric | Value |
|---|---|
| Total rows | 265,121 |
| Earliest date | 1999-01-04 |
| Latest date | 2026-06-24 |
| Active currencies | 32 |
| Discontinued currencies | 14 |
| Years covered | 28 |

## Currency coverage

### Active currencies (present through to latest load date)

| Currency | From | Trading days |
|---|---|---|
| AUD | 1999-01-04 | 7,061 |
| BRL | 2000-01-13 | 6,793 |
| CAD | 1999-01-04 | 7,061 |
| CHF | 1999-01-04 | 7,061 |
| CNY | 2000-01-13 | 6,793 |
| CZK | 1999-01-04 | 7,061 |
| DKK | 1999-01-04 | 7,061 |
| GBP | 1999-01-04 | 7,061 |
| HKD | 1999-01-04 | 7,061 |
| HUF | 1999-01-04 | 7,061 |
| IDR | 1999-01-04 | 7,061 |
| ILS | 2000-01-13 | 6,792 |
| INR | 2000-01-13 | 6,793 |
| ISK | 1999-01-04 | 4,710 |
| JPY | 1999-01-04 | 7,061 |
| KRW | 1999-01-04 | 7,061 |
| MXN | 1999-01-04 | 7,061 |
| MYR | 1999-01-04 | 7,061 |
| NOK | 1999-01-04 | 7,061 |
| NZD | 1999-01-04 | 7,061 |
| PHP | 1999-01-04 | 7,061 |
| PLN | 1999-01-04 | 7,061 |
| RON | 1999-01-04 | 7,061 |
| SEK | 1999-01-04 | 7,061 |
| SGD | 1999-01-04 | 7,061 |
| THB | 1999-01-04 | 7,061 |
| TRY | 1999-01-04 | 7,061 |
| TWD | 2000-01-13 | — dropped 2020 |
| USD | 1999-01-04 | 7,061 |
| ZAR | 1999-01-04 | 7,061 |

### Discontinued currencies (no longer published by ECB)

These are present in the data for historical periods but have no recent rates.
Most were discontinued when the country joined the Eurozone or the ECB stopped reporting them.

| Currency | From | To | Reason |
|---|---|---|---|
| ARS | 2000-01-13 | 2020-10-30 | Dropped from ECB reporting |
| BGN | 2000-07-19 | 2025-12-31 | Bulgaria joined Eurozone Jan 2026 |
| CYP | 1999-01-04 | 2007-12-31 | Cyprus joined Eurozone 2008 |
| DZD | 2000-01-13 | 2020-10-30 | Dropped from ECB reporting |
| EEK | 1999-01-04 | 2010-12-31 | Estonia joined Eurozone 2011 |
| GRD | 1999-01-04 | 2000-12-29 | Greece joined Eurozone 2001 |
| HRK | 2000-01-13 | 2022-12-30 | Croatia joined Eurozone 2023 |
| LTL | 1999-01-04 | 2014-12-31 | Lithuania joined Eurozone 2015 |
| LVL | 1999-01-04 | 2013-12-31 | Latvia joined Eurozone 2014 |
| MAD | 2000-01-13 | 2020-10-30 | Dropped from ECB reporting |
| MTL | 1999-01-04 | 2007-12-31 | Malta joined Eurozone 2008 |
| ROL | 1999-01-04 | 2005-06-30 | Replaced by RON (redenomination) |
| RUB | 1999-01-04 | 2022-03-01 | Dropped after Ukraine invasion sanctions |
| SIT | 1999-01-04 | 2006-12-29 | Slovenia joined Eurozone 2007 |
| SKK | 1999-01-04 | 2008-12-31 | Slovakia joined Eurozone 2009 |
| TRL | 1999-01-04 | 2004-12-31 | Replaced by TRY (redenomination) |

## Known issues & caveats

- Rates are ECB reference rates, published at ~16:00 CET each trading day. They
  are indicative mid-market rates, not transaction rates.
- Weekends and ECB public holidays have no entry — do not assume gaps are missing
  data, they are non-trading days.
- The base is always EUR. To convert between two non-EUR currencies, divide their
  respective rates (e.g. GBP/USD = rate_USD / rate_GBP).
- Coverage starts 1999-01-04; earlier dates are not available.
- TWD (Taiwan Dollar) and several others (ARS, DZD, MAD) were dropped from ECB
  reporting around October 2020 with no explanation.
- RUB (Russian Ruble) has no entries after 2022-03-01, reflecting sanctions following
  the invasion of Ukraine.
- BGN (Bulgarian Lev) disappears after 2025-12-31 as Bulgaria joined the Eurozone
  on 1 January 2026.
- ISK (Icelandic Króna) has 4,710 trading days vs 7,061 for full-history currencies,
  suggesting gaps in ECB reporting — treat with caution for time-series analysis.
- ROL and TRL are legacy codes replaced by RON and TRY respectively after currency
  redenominations; both old and new codes are present in the data, covering
  non-overlapping periods.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
