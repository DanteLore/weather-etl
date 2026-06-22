# ONS Postcode Lookup

## Description

Lookup table mapping every live postcode in England and Wales to its
administrative geographies: MSOA, local authority district (LAD), county/unitary
authority (CTYUA), region, and country. Useful for answering "what local
authority is postcode X in" or for aggregating postcode-level data to any of
these higher geographies. Sourced from the ONS ArcGIS FeatureServer. Covers
approximately 2.35 million postcodes.

## Classification

Public. Open government data; no personal data.

## Owner

Dan. See gov-etl repo for the code.

## Source & references

ONS ArcGIS FeatureServer:
`Postcode_to_OA_(2021)_to_LSOA_to_MSOA_to_LAD_to_CTYUA_to_RGN_to_CTRY_Best_Fit_Lookup_in_EW`

Boundary vintages: MSOA 2021, LAD 2022, CTYUA 2022, RGN 2022.

## License & usage restrictions

Open Government Licence v3.0. Free to use, share and adapt with attribution
to ONS.

## Location

- S3: `s3://dantelore.data.incoming/ons_postcode_lookup/lookup/postcode_area=<xx>/`
- Glue: `incoming.ons_postcode_lookup_lookup`
- Format: Parquet, partitioned by `postcode_area` (lowercase postcode district
  letters, e.g. `rg`, `sw`)

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| postcode | string | No | Primary | Full postcode as returned by ONS (may include space) |
| postcode_area | string | No | Partition key | Lowercase leading letters of postcode, e.g. `rg` |
| msoa21cd | string | Yes | Joins to MSOA boundaries | MSOA 2021 code |
| msoa21nm | string | Yes | | MSOA 2021 name |
| lad22cd | string | Yes | | Local authority district code (2022) |
| lad22nm | string | Yes | | Local authority district name |
| ctyua22cd | string | Yes | | County/unitary authority code (2022) |
| ctyua22nm | string | Yes | | County/unitary authority name |
| rgn22cd | string | Yes | | Region code (2022) |
| rgn22nm | string | Yes | | Region name |
| ctry22cd | string | Yes | | Country code |
| ctry22nm | string | Yes | | Country name |

## Common queries

### Look up a single postcode

```sql
SELECT *
FROM ons_postcode_lookup_lookup
WHERE postcode_area = 'rg'
  AND postcode = 'RG14 5RU'
```

### All postcodes in an MSOA

```sql
SELECT postcode
FROM ons_postcode_lookup_lookup
WHERE postcode_area = 'rg'
  AND msoa21cd = 'E02003552'
ORDER BY postcode
```

### Join house prices to MSOA income

```sql
SELECT
    p.postcode, p.price, p.property_type,
    l.msoa21nm,
    i.net_annual_income, i.net_income_after_housing
FROM house_prices_ppd p
JOIN ons_postcode_lookup_lookup l
    ON p.postcode = l.postcode
    AND l.postcode_area = LOWER(REGEXP_EXTRACT(p.postcode, '^([A-Za-z]+)'))
JOIN ons_msoa_income_income i ON l.msoa21cd = i.msoa21cd
WHERE p.year = '2023'
  AND p.town_city = 'NEWBURY'
ORDER BY i.net_annual_income DESC
```

### Average house price by MSOA for a county

```sql
SELECT
    l.msoa21nm,
    COUNT(*) AS sales,
    ROUND(AVG(p.price)) AS avg_price,
    i.net_annual_income
FROM house_prices_ppd p
JOIN ons_postcode_lookup_lookup l
    ON p.postcode = l.postcode
    AND l.postcode_area = LOWER(REGEXP_EXTRACT(p.postcode, '^([A-Za-z]+)'))
JOIN ons_msoa_income_income i ON l.msoa21cd = i.msoa21cd
WHERE p.year = '2023'
  AND l.ctyua22nm = 'West Berkshire'
GROUP BY l.msoa21nm, i.net_annual_income
ORDER BY avg_price DESC
```

### Postcode count per MSOA (proxy for density)

```sql
SELECT msoa21cd, msoa21nm, COUNT(*) AS postcode_count
FROM ons_postcode_lookup_lookup
WHERE postcode_area IN ('rg', 'ox')
GROUP BY msoa21cd, msoa21nm
ORDER BY postcode_count DESC
LIMIT 20
```

## Source ETL / code

`github.com/dantelore/gov-etl`, `ons_postcode_lookup/ons_postcode_lookup_load.py`.
Fetches ~2.35M records from the ArcGIS FeatureServer in parallel (20 concurrent workers,
1,000 records per page). Expect ~5–10 minutes to complete.

## Freshness & update cadence

Loaded on demand; no automatic refresh. The ONS releases updated boundary
lookups periodically - check ONS Open Geography Portal for new versions before
relying on this for anything boundary-sensitive.

## Known issues & caveats

- England and Wales only - no Scotland or Northern Ireland postcodes.
- Boundary vintages are mixed: MSOA 2021, LAD/CTYUA/RGN 2022. Joining to
  datasets that use different boundary vintages will silently produce wrong
  results.
- Some geography fields are nullable; postcodes with no assigned geography
  (e.g. non-geographic postcodes) will have nulls in the code/name fields.
- The ArcGIS service caps pages at 1,000 records; the ETL works around this
  with concurrent pagination.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
