# ONS MSOA Income Estimates

## Description

Modelled household income estimates for all 7,264 MSOAs in England and Wales, financial
year ending March 2023. Published by ONS as part of the Small Area Income Estimates series.
Provides four income measures (total gross, net disposable, net before housing costs, net
after housing costs) each with 95% confidence intervals. The key dataset for understanding
income distribution at neighbourhood level.

## Classification

Public. Official government statistics; no personal data (modelled area estimates, not
individual records).

## Owner

Dan. See gov-etl repo for the code.

## Source & references

ONS Small Area Income Estimates for MSOAs, England and Wales — financial year ending 2023.
Published December 2025.

## License & usage restrictions

Open Government Licence v3.0. Free to use with attribution to ONS.

## Location

- S3: `s3://dantelore.data.incoming/ons_msoa_income/income/msoa_income_2023.parquet`
- Glue: `incoming.ons_msoa_income_income`
- Format: Single Parquet file, no partitioning (7,264 rows)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| msoa21cd | string | No | Primary; joins to ons_msoa_boundaries_msoa and ons_postcode_lookup_lookup | MSOA 2021 code |
| msoa21nm | string | No | | MSOA name |
| lad_code | string | No | | Local Authority District code |
| lad_name | string | No | | Local Authority District name |
| rgn_code | string | No | | Region code |
| rgn_name | string | No | | Region name |
| total_annual_income | double | Yes | | Total gross household income (£) |
| total_annual_income_upper_ci | double | Yes | | Upper 95% confidence limit (£) |
| total_annual_income_lower_ci | double | Yes | | Lower 95% confidence limit (£) |
| total_annual_income_ci | double | Yes | | Confidence interval half-width (£) |
| net_annual_income | double | Yes | | Disposable net household income (£) |
| net_annual_income_upper_ci | double | Yes | | Upper 95% confidence limit (£) |
| net_annual_income_lower_ci | double | Yes | | Lower 95% confidence limit (£) |
| net_annual_income_ci | double | Yes | | Confidence interval half-width (£) |
| net_income_before_housing | double | Yes | | Equivalised net income before housing costs (£) |
| net_income_before_housing_upper_ci | double | Yes | | Upper 95% confidence limit (£) |
| net_income_before_housing_lower_ci | double | Yes | | Lower 95% confidence limit (£) |
| net_income_before_housing_ci | double | Yes | | Confidence interval half-width (£) |
| net_income_after_housing | double | Yes | | Equivalised net income after housing costs (£) |
| net_income_after_housing_upper_ci | double | Yes | | Upper 95% confidence limit (£) |
| net_income_after_housing_lower_ci | double | Yes | | Lower 95% confidence limit (£) |
| net_income_after_housing_ci | double | Yes | | Confidence interval half-width (£) |

## Which measure to use

- **`net_income_after_housing`** — best proxy for disposable living standards; accounts for both tax/benefits and housing costs. Use when comparing affordability across areas.
- **`net_annual_income`** — useful when housing costs are separately modelled (e.g. joining to house price data to compute affordability ratios).
- **`total_annual_income`** — gross income; use when benchmarking against pre-tax earnings data.
- **Confidence intervals** — MSOAs with wide CIs (large `_ci` values relative to the estimate) have high uncertainty; treat those figures with caution.

## Common queries

### Highest income MSOAs nationally

```sql
SELECT msoa21nm, lad_name, rgn_name,
       ROUND(net_annual_income) AS net_income,
       ROUND(net_income_after_housing) AS net_after_housing
FROM ons_msoa_income_income
ORDER BY net_annual_income DESC
LIMIT 20
```

### Income distribution within a local authority

```sql
SELECT
    msoa21nm,
    ROUND(net_annual_income)        AS net_income,
    ROUND(net_income_after_housing) AS net_after_housing,
    ROUND(net_annual_income_ci)     AS uncertainty
FROM ons_msoa_income_income
WHERE lad_name = 'West Berkshire'
ORDER BY net_annual_income DESC
```

### Regional average incomes

```sql
SELECT
    rgn_name,
    ROUND(AVG(net_annual_income))        AS avg_net_income,
    ROUND(AVG(net_income_after_housing)) AS avg_net_after_housing,
    COUNT(*) AS msoa_count
FROM ons_msoa_income_income
GROUP BY rgn_name
ORDER BY avg_net_income DESC
```

### House price to income ratio by MSOA

```sql
SELECT
    i.msoa21nm,
    i.lad_name,
    ROUND(approx_percentile(p.price, 0.5))                           AS median_house_price,
    ROUND(i.net_annual_income)                                       AS net_household_income,
    ROUND(approx_percentile(p.price, 0.5) / i.net_annual_income, 1) AS price_to_income_ratio
FROM house_prices_ppd p
JOIN ons_postcode_lookup_lookup l
    ON p.postcode = l.postcode
    AND l.postcode_area = LOWER(REGEXP_EXTRACT(p.postcode, '^([A-Za-z]+)'))
JOIN ons_msoa_income_income i ON l.msoa21cd = i.msoa21cd
WHERE p.year = '2023'
  AND i.lad_name = 'West Berkshire'
GROUP BY i.msoa21nm, i.lad_name, i.net_annual_income
ORDER BY price_to_income_ratio DESC
```

### Income vs rural-urban classification

```sql
SELECT
    b.ruc21nm,
    COUNT(*) AS msoa_count,
    ROUND(AVG(i.net_annual_income))        AS avg_net_income,
    ROUND(AVG(i.net_income_after_housing)) AS avg_net_after_housing
FROM ons_msoa_income_income i
JOIN ons_msoa_boundaries_msoa b ON i.msoa21cd = b.msoa21cd
GROUP BY b.ruc21nm
ORDER BY avg_net_income DESC
```

## Source ETL / code

`github.com/dantelore/gov-etl`, `ons_msoa_income/ons_msoa_income_load.py`.

## Freshness & update cadence

ONS publish updated estimates every 2–3 years. When a new edition is available, update
`DOWNLOAD_URL` in the loader and re-run.

## Known issues & caveats

- Modelled estimates, not direct measurements — MSOAs with wide confidence intervals
  (large `_ci` values relative to the estimate) have high uncertainty.
- England and Wales only.
- "Equivalised" income adjusts for household size/composition — a single person and a
  family of four with the same raw income will have different equivalised incomes.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
