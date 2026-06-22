# VOA 2026 Rating List

## Description

All non-domestic (commercially rated) properties in England and Wales, from the Valuation
Office Agency 2026 compiled rating list. 2.14 million entries covering every hereditament
(unit of property liable for business rates) — shops, offices, warehouses, factories, pubs,
schools, car parks, advertising hoardings and more. Useful for understanding the commercial
footprint of any area.

## Classification

Public. Published by a government agency; no personal data.

## Owner

Dan. See gov-etl repo for the code.

## Source & references

Valuation Office Agency — 2026 Compiled Rating List, epoch 0001.
https://voaratinglists.blob.core.windows.net/html/rlidata.htm

## License & usage restrictions

VOA terms and conditions. Personal and research use permitted. Not for commercial resale.

## Location

- S3: `s3://dantelore.data.incoming/voa_rating_list/entries/postcode_area=<xx>/`
- Glue: `incoming.voa_rating_list_entries`
- Format: Parquet, partitioned by `postcode_area` (lowercase leading letters of postcode, e.g. `rg`)

Always filter on `postcode_area` to avoid a full table scan.

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| uarn | string | No | Primary | Unique Address Reference Number — VOA's property identifier |
| ba_code | string | Yes | | Billing authority (local council) code |
| ndr_community_code | string | Yes | | NDR community code |
| desc_code | string | Yes | | Property type code (e.g. `CS` = shop, `CO` = office) |
| desc_text | string | Yes | | Property type description (e.g. `SHOP AND PREMISES`) |
| assessment_reference | bigint | Yes | | Internal VOA assessment reference |
| full_address | string | Yes | | Full address as a single concatenated string |
| number_or_name | string | Yes | | Property number or name |
| street | string | Yes | | Street name |
| town | string | Yes | | Town |
| postal_district | string | Yes | | Postal district |
| county | string | Yes | | County |
| postcode | string | Yes | Joins to os_code_point_open_codepo for coordinates | Full postcode (e.g. `RG14 5RU`) |
| effective_date | string | Yes | | Date entry became effective (DD-MON-YYYY) |
| rateable_value | int | Yes | | Rateable value in £ |
| appeal_settlement_code | string | Yes | | Appeal status code |
| ba_reference | string | Yes | | Billing authority's own reference |
| list_alteration_date | string | Yes | | Date the list entry was last altered (DD-MON-YYYY) |
| scat_code | string | Yes | | Special Category code — more granular than desc_code |
| sub_street_1 | string | Yes | | Sub-street address level 1 (e.g. building name) |
| sub_street_2 | string | Yes | | Sub-street address level 2 (e.g. floor) |
| sub_street_3 | string | Yes | | Sub-street address level 3 (e.g. unit) |
| case_number | bigint | Yes | | Appeal case number if applicable |
| current_from_date | string | Yes | | Date current rateable value took effect (DD-MON-YYYY) |
| postcode_area | string | No | Partition key | Lowercase leading letters of postcode (e.g. `rg`) |

## Common property type codes (`desc_code`)

| Code | Description | Count (2026 epoch 0001) |
|---|---|---|
| `CS` | Shop and premises | 418,620 |
| `CO` | Offices and premises | 363,710 |
| `IF3` | Workshop and premises | 198,482 |
| `CW` | Warehouse and premises | 127,132 |
| `CW3` | Store and premises | 83,578 |
| `CH1` | Self catering holiday unit | 72,797 |
| `CP1` | Car parking space | 41,745 |
| `CL` | Public house and premises | 38,079 |
| `MT1` | Communication station | 35,075 |
| `CR` | Restaurant and premises | 32,605 |
| `IF` | Factory and premises | 27,078 |
| `CA` | Advertising right | 26,679 |
| `EL` | School and premises | 22,386 |

The `scat_code` column provides further granularity within each type.

## Partition tip

Always filter on `postcode_area` to get partition pruning — filtering on `postcode` alone
causes a full 2.14M-row scan:

```sql
-- Good: partition filter
SELECT * FROM voa_rating_list_entries WHERE postcode_area = 'rg'

-- Bad: scans all partitions
SELECT * FROM voa_rating_list_entries WHERE postcode LIKE 'RG%'
```

## Common queries

### Count commercial properties in a postcode area

```sql
SELECT COUNT(*) AS commercial_count
FROM voa_rating_list_entries
WHERE postcode_area = 'rg'
```

### Count by property type

```sql
SELECT desc_code, desc_text, COUNT(*) AS cnt
FROM voa_rating_list_entries
WHERE postcode_area = 'rg'
GROUP BY desc_code, desc_text
ORDER BY cnt DESC
```

### Highest rateable values in an area

```sql
SELECT desc_text, full_address, postcode, rateable_value
FROM voa_rating_list_entries
WHERE postcode_area = 'rg'
ORDER BY rateable_value DESC
LIMIT 20
```

### Filter to specific property types

```sql
SELECT number_or_name, street, town, postcode, rateable_value
FROM voa_rating_list_entries
WHERE postcode_area = 'rg'
  AND desc_code IN ('CS', 'CR')
ORDER BY town, street
```

### Join to OS Code Point Open for coordinates

VOA postcodes are uppercase with a space (e.g. `RG14 5RU`) — same format as Code Point Open.
Include `postcode_area` on both sides for partition pruning on both tables:

```sql
SELECT
    v.uarn, v.desc_code, v.desc_text, v.full_address, v.postcode, v.rateable_value,
    c.eastings, c.northings
FROM voa_rating_list_entries v
JOIN os_code_point_open_codepo c
    ON  c.postcode      = v.postcode
    AND c.postcode_area = v.postcode_area
WHERE v.postcode_area = 'rg'
```

### Total commercial rateable value by town

```sql
SELECT town, COUNT(*) AS commercial_count, SUM(rateable_value) AS total_rateable_value
FROM voa_rating_list_entries
WHERE postcode_area IN ('rg', 'ox', 'sl')
GROUP BY town
ORDER BY commercial_count DESC
```

## Source ETL / code

`github.com/dantelore/gov-etl`, `voa_rating_list/voa_rating_list_load.py`.

## Freshness & update cadence

The VOA publish change updates twice weekly. The loaded baseline is epoch `0001` of the
2026 rating list (effective April 2026). Re-run the loader to refresh; update `DOWNLOAD_URL`
in the script if a newer epoch is available.

## Known issues & caveats

- 1,054 rows have a null postcode; these are stored in a separate `unknown` partition.
- Dates are stored as strings in `DD-MON-YYYY` format (e.g. `01-APR-2026`), not ISO dates.
- `desc_code` is a VOA internal code; the mapping to human-readable types is in `desc_text` alongside it.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
