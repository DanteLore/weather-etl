# Census 2021 Home to Workplace Flows (ODWP01EW)

## Description

Census 2021 origin-destination flows: how many working residents of each area work in each other area. One row per home area, workplace area and place-of-work category. Loaded at MSOA level (1.86 million flows) and local authority level (75,000 flows) for all of England and Wales. Shows where residents actually commute to, e.g. how many people living in Thatcham work in Newbury, Reading, Basingstoke or London.

## Classification

Public. Aggregated Census data; no personal data.

## Owner

Dan. See the gov-etl repo for the code.

## Source & references

ONS Census 2021 origin-destination data, table ODWP01EW "Location of usual residence and place of work", via the Nomis bulk download `odwp01ew.zip` (released 26 October 2023). https://www.nomisweb.co.uk/sources/census_2021_od

## License & usage restrictions

Open Government Licence v3.0. Attribute ONS / Nomis.

## Location

- S3: `s3://dantelore.data.incoming/nomis_census_commute_flows/geography_level=<level>/nomis_census_commute_flows_<level>.parquet`
- Glue: `incoming.nomis_census_commute_flows`
- Format: Parquet, partitioned by `geography_level`: `msoa`, `ltla`

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| home_code | string | No | Key | Area of usual residence: MSOA code (`E02…`) or local authority code. Joins to `ons_msoa_boundaries_msoa.msoa21cd` |
| home_name | string | No | | Home area name, e.g. `West Berkshire 016` |
| workplace_code | string | No | Key | Workplace area code. Special values: `-8` does not apply (not in employment), `999999999` outside the UK, `888888888` offshore installation |
| workplace_name | string | No | | Workplace area name, or `Does not apply`, `Workplace is outside the UK`, `Workplace is offshore installation` |
| place_of_work_code | int | No | | `-8` does not apply (not in employment), `1` mainly at or from home / no fixed place, `2` offshore or outside the UK, `3` working in the UK at a workplace |
| place_of_work_name | string | No | | Description of `place_of_work_code` |
| count | int | No | | Usual residents aged 16+ |
| geography_level | string | No | Partition key | `msoa` or `ltla` |

The population is all usual residents aged 16 and over, so each home area's rows add up to that, with those not in employment under `-8`. Home workers appear as a flow to their own area with `place_of_work_code = 1`.

## Common queries

Where Thatcham residents work (Thatcham's MSOAs are West Berkshire 011, 014, 016 and 017; 011 also covers Colthrop and Crookham to the east):

```sql
select workplace_name, sum(count) as people
from incoming.nomis_census_commute_flows
where geography_level = 'msoa'
  and home_code in ('E02003377', 'E02003380', 'E02003382', 'E02003383')
  and place_of_work_code = 3
group by 1 order by 2 desc limit 20
```

By local authority, for the bigger picture:

```sql
select workplace_name, sum(count) as people
from incoming.nomis_census_commute_flows
where geography_level = 'ltla' and home_name = 'West Berkshire' and place_of_work_code = 3
group by 1 order by 2 desc
```

## Source ETL / code

`github.com/dantelore/gov-etl`, `nomis_census_commute_flows/nomis_census_commute_flows_load.py`.

## Freshness & update cadence

Census 2021 snapshot, loaded 2026-11-12. Static.

## Running costs (estimated)

From the S3 size on 2026-11-12. Method: [gov-etl README](README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.008 GB, 2 objects | <$0.001 |
| **Total** | | **<$0.001** |

Querying it: the MSOA partition is under 8 MB.

## Known issues & caveats

- **Census 2021 was taken during lockdown.** Many commuters were working from home that week, so flows to distant workplaces (London, Reading) are understated against a normal year and home working overstated. See [nomis_census_travel_method](nomis_census_travel_method.md).
- The output area level (1 GB of CSV) wasn't loaded. Upper tier authorities and regions weren't either; they can be summed from the local authority rows.
- Small flows are perturbed by ONS disclosure control; treat flows of a handful of people as approximate.
- MSOA names are the ONS codes' official names (`West Berkshire 016`), not the House of Commons Library's descriptive names.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|
| Thatcham analysis (thatchamplan.co.uk) | Dan | Where Thatcham residents commute to |

## Status

Active.
