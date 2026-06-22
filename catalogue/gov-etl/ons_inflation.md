# ONS Inflation (MM23)

## Description

Monthly UK inflation time series from the ONS MM23 dataset, covering CPI, CPIH and RPI
from as far back as June 1948. One row per calendar month. Primarily useful for deflating
nominal monetary values (house prices, rateable values) to real terms for comparison across
time periods.

## Classification

Public. Official government statistics; no personal data.

## Owner

Dan. See gov-etl repo for the code.

## Source & references

ONS MM23 dataset, fetched via the ONS time series API.
Series used: CPI index (`l522`), CPI rate (`d7g7`), CPIH index (`l55o`), CPIH rate (`d7bt`), RPI rate (`czbh`).

## License & usage restrictions

Open Government Licence v3.0. Free to use with attribution to ONS.

## Location

- S3: `s3://dantelore.data.incoming/ons_inflation/inflation/inflation.parquet`
- Glue: `incoming.ons_inflation_inflation`
- Format: Single Parquet file, no partitioning (934 rows — always scanned in full)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| year | int | No | Primary (with month) | Calendar year |
| month | int | No | Primary (with month) | Month number (1–12) |
| cpi_index | double | Yes | | CPI index value (2015=100); from Jan 1988 |
| cpi_rate | double | Yes | | CPI 12-month rate % (year-on-year); from Jan 1989 |
| cpih_index | double | Yes | | CPIH index value (2015=100); from Jan 1989 |
| cpih_rate | double | Yes | | CPIH 12-month rate % (year-on-year); from Jan 1988 |
| rpi_rate | double | Yes | | RPI 12-month rate % (year-on-year); from Jun 1948 |

To deflate a nominal price to 2015 pounds: `real_price = nominal_price / cpi_index * 100`.

## Which measure to use

- **CPI** — the UK's headline inflation measure since 2003, used by the Bank of England for its 2% target. Best for deflating recent data (1988+).
- **CPIH** — CPI plus owner-occupiers' housing costs. ONS's preferred measure since 2017. Slightly smoother than CPI.
- **RPI** — older measure, no longer a National Statistic but still widely used in contracts and index-linking. The only series with data before 1988.

## Common queries

### Latest inflation figures

```sql
SELECT *
FROM ons_inflation_inflation
ORDER BY year DESC, month DESC
LIMIT 6
```

### Annual average CPI rate by year

```sql
SELECT year, ROUND(AVG(cpi_rate), 2) AS avg_cpi_rate
FROM ons_inflation_inflation
WHERE cpi_rate IS NOT NULL
GROUP BY year
ORDER BY year DESC
```

### Years with highest inflation (CPI)

```sql
SELECT year, ROUND(AVG(cpi_rate), 1) AS avg_cpi_rate
FROM ons_inflation_inflation
WHERE cpi_rate IS NOT NULL
GROUP BY year
ORDER BY avg_cpi_rate DESC
LIMIT 10
```

### Deflating house prices to real terms (2015 £)

Join on year and month to bring in the CPI index. Note: `date_of_transfer` contains a time
component (`2010-12-02 00:00`), so use `SUBSTR(..., 6, 2)` to extract the month.

```sql
SELECT
    p.year,
    p.town_city,
    p.postcode,
    p.price                                             AS nominal_price,
    i.cpi_index,
    ROUND(CAST(p.price AS double) / i.cpi_index * 100) AS real_price_2015
FROM house_prices_ppd p
JOIN ons_inflation_inflation i
    ON  CAST(p.year AS int) = i.year
    AND CAST(SUBSTR(p.date_of_transfer, 6, 2) AS int) = i.month
WHERE p.year = '2010'
  AND p.town_city = 'NEWBURY'
ORDER BY real_price_2015 DESC
LIMIT 20
```

### Cumulative inflation between two dates

How much has £100 in January 2000 grown to in today's money?

```sql
WITH base AS (
    SELECT cpi_index FROM ons_inflation_inflation WHERE year = 2000 AND month = 1
),
latest AS (
    SELECT cpi_index FROM ons_inflation_inflation ORDER BY year DESC, month DESC LIMIT 1
)
SELECT ROUND(latest.cpi_index / base.cpi_index * 100, 2) AS equivalent_value
FROM base, latest
```

## Source ETL / code

`github.com/dantelore/gov-etl`, `ons_inflation/ons_inflation_load.py`.
Series IDs: `l522` (CPI index), `d7g7` (CPI rate), `l55o` (CPIH index), `d7bt` (CPIH rate), `czbh` (RPI rate).

## Freshness & update cadence

ONS publish updated figures monthly, typically on the third Wednesday. Re-run the loader
to append the latest month. The file is replaced in full on each run.

## Known issues & caveats

- Series start dates vary: RPI from Jun 1948, CPI/CPIH from Jan 1988/1989. Earlier months
  will have nulls for the later series.
- RPI is no longer a National Statistic (since 2013) but is retained as it's still widely
  used in contracts and index-linking. Prefer CPI or CPIH for new analysis.
- No RPI index value is stored, only the rate. To reconstruct an index you would need to
  chain the monthly rates from a known base.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
