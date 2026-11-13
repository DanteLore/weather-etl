# ONS 2011 to 2021 Census Area Lookups

## Description

How 2011 Census output areas, LSOAs and MSOAs map to the 2021 ones, so 2011 and 2021 Census results can be compared on the same areas. England and Wales. One row per 2011/2021 pair: an area that was split appears once for each 2021 part, and merged areas share a 2021 code.

## Classification

Public. No personal data.

## Owner

Dan. See the gov-etl repo for the code.

## Source & references

ONS Open Geography Portal: "Output Area (2011) to Output Area (2021) to Local Authority District (2022) Exact Fit Lookup", "LSOA (2011) to LSOA (2021) to LAD (2022)" and "MSOA (2011) to MSOA (2021) to LAD (2022)" lookups (ArcGIS services `OA11_OA21_LAD22_EW_LU_Exact_fit_V3`, `LSOA11_LSOA21_LAD22_EW_LU_v5`, `MSOA11_MSOA21_LAD22_EW_LU_v2`). https://geoportal.statistics.gov.uk/

## License & usage restrictions

Open Government Licence v3.0. Source: Office for National Statistics licensed under the OGL. Contains OS data © Crown copyright and database right.

## Location

- S3: `s3://dantelore.data.incoming/ons_census_area_lookup_2011_2021/geography_level=<level>/ons_census_area_lookup_2011_2021_<level>.parquet`
- Glue: `incoming.ons_census_area_lookup_2011_2021`
- Format: Parquet, partitioned by `geography_level`: `oa`, `lsoa`, `msoa`

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| code_2011 | string | No | Key | 2011 area code |
| name_2011 | string | Yes | | 2011 area name (LSOA and MSOA only) |
| code_2021 | string | No | Key | 2021 area code |
| name_2021 | string | Yes | | 2021 area name (LSOA and MSOA only) |
| change_indicator | string | Yes | | `U` unchanged, `S` split (one 2011 area became several), `M` merged (several became one), `X` irregular redesign (complex change). Null for MSOAs (see caveats) |
| lad22_code, lad22_name | string | No | | Local authority (2022) |
| geography_level | string | No | Partition key | `oa`, `lsoa`, `msoa` |

On 2026-11-12: 189,567 OA pairs (175,469 unchanged), 35,796 LSOA pairs (33,647 unchanged), 7,201 MSOAs.

## Common queries

Census 2011 cycling to work re-aggregated onto 2021 LSOAs (safe where areas were unchanged or merged; for splits, the 2011 count is repeated for each part):

```sql
select l.code_2021, l.name_2021, l.change_indicator, sum(t.bicycle) as cyclists_2011
from incoming.ons_census_area_lookup_2011_2021 l
join incoming.nomis_census_2011_travel_method t
  on t.geography_level = 'lsoa' and t.geography_code = l.code_2011
where l.geography_level = 'lsoa' and l.lad22_name = 'West Berkshire'
group by 1, 2, 3
```

Output areas in West Berkshire that changed:

```sql
select change_indicator, count(*) from incoming.ons_census_area_lookup_2011_2021
where geography_level = 'oa' and lad22_name = 'West Berkshire' group by 1
```

## Source ETL / code

`github.com/dantelore/gov-etl`, `ons_census_area_lookup/ons_census_area_lookup_load.py`.

```bash
python ons_census_area_lookup/ons_census_area_lookup_load.py
```

## Freshness & update cadence

Static lookups, loaded 2026-11-12.

## Running costs (estimated)

From the S3 size on 2026-11-12. Method: [gov-etl README](README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.003 GB, 3 objects | <$0.001 |
| **Total** | | **<$0.001** |

## Known issues & caveats

- **The MSOA lookup is best fit, one row per 2011 MSOA**, with no change indicator: 7,201 rows against 7,264 MSOAs in 2021, so MSOAs split in 2021 map to only one of their parts. For exact comparisons, go via LSOAs or output areas.
- Split areas: a 2011 count can't be divided between the 2021 parts without more data; aggregate up to the 2011 area instead (or to a group of areas both censuses share).
- The OA lookup has one row with indicator `x` (lower case) as published.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|
| Thatcham analysis (thatchamplan.co.uk) | Dan | Comparing Census 2011 and 2021 travel to work |

## Status

Active.
