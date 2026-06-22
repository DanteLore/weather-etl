# Land Registry Price Paid Data

## Description

Every residential property sale registered with HM Land Registry in England
and Wales, going back to 1995. Includes sale price, date, address, property
type, whether new build, and tenure. Commonly known as "Price Paid Data" or
"PPD." Useful for house price analysis, area-level affordability, and joining
to postcode or geography lookups.

## Classification

Public. Open government data; individual transactions are published openly by
HM Land Registry. No personal data (buyers/sellers are not included).

## Owner

Dan. See gov-etl repo for the code.

## Source & references

HM Land Registry Price Paid Data:
https://www.gov.uk/guidance/about-the-price-paid-data

Download URL pattern: `https://price-paid-data.publicdata.landregistry.gov.uk/pp-<YYYY>.csv`

## License & usage restrictions

Open Government Licence v3.0. Free to use, share and adapt with attribution
to HM Land Registry.

## Location

- S3: `s3://dantelore.data.incoming/house_prices/ppd/year=<YYYY>/`
- Glue: `incoming.house_prices_ppd`
- Format: Parquet, partitioned by `year`

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| transaction_id | string | No | Primary | Land Registry transaction unique identifier |
| price | bigint | Yes | | Sale price in GBP |
| date_of_transfer | string | No | | Date of sale, format varies (stored as string from source) |
| postcode | string | Yes | Joins to `gov-etl/ons_postcode_lookup` | Property postcode |
| property_type | string | No | | D=Detached, S=Semi-detached, T=Terraced, F=Flat/Maisonette, O=Other |
| old_new | string | No | | Y=new build, N=established residential |
| duration | string | No | | F=Freehold, L=Leasehold, U=Unknown |
| paon | string | Yes | | Primary Addressable Object Name (house number or name) |
| saon | string | Yes | | Secondary Addressable Object Name (flat number etc) |
| street | string | Yes | | Street name |
| locality | string | Yes | | Locality |
| town_city | string | Yes | | Town or city |
| district | string | Yes | | District |
| county | string | Yes | | County |
| ppd_category_type | string | No | | A=standard, B=additional (non-residential, buy-to-let, etc) |
| record_status | string | No | | A=Addition, C=Change, D=Delete (for incremental files) |

## Source ETL / code

`github.com/dantelore/gov-etl`, `house_prices/house_prices_bulk_load.py`.
Run manually to bulk-load one or more years:

```
python house_prices/house_prices_bulk_load.py --start 2010
python house_prices/house_prices_bulk_load.py --start 2024 --end 2024
```

## Freshness & update cadence

Bulk-loaded by year, manually triggered. Land Registry publishes monthly
updates and a full annual file each year. There is no automatic refresh - run
the script to pick up new years.

## Known issues & caveats

- The source CSV files have no header row; column order is fixed per Land
  Registry schema. The ETL assigns column names during load.
- All fields are loaded as strings initially; `price` is cast to integer in the
  ETL, but all other fields including `date_of_transfer` remain as strings.
  Cast at query time.
- `ppd_category_type = 'B'` records include sales that are not standard
  residential (commercial sales, buy-to-let portfolios, etc.). Filter to
  `ppd_category_type = 'A'` for typical house price analysis.
- `record_status` is meaningful in incremental monthly files but all bulk
  annual loads contain `A` (addition) records only.
- England and Wales only; Scotland and Northern Ireland use separate registries.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
