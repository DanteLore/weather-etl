# NaPTAN Public Transport Stops

## Description

The National Public Transport Access Nodes (NaPTAN) database: every bus stop, bus station, railway station entrance and platform, tram and metro stop, ferry terminal and taxi rank in Great Britain, with its location. 435,811 stops, including inactive ones. Each stop has its ATCO code (the ID used in bus timetables and live bus data), name, street, indicator (e.g. `opp`, `Stop A`), locality and coordinates. Useful for walking distance to the nearest bus stop and for decoding stop IDs in the BODS bus tables.

## Classification

Public. Open government data; no personal data.

## Owner

Dan. See the gov-etl repo for the code.

## Source & references

Department for Transport NaPTAN API, full CSV download: `https://naptan.api.dft.gov.uk/v1/access-nodes?dataFormat=csv`. Guide: https://www.gov.uk/government/publications/national-public-transport-access-node-schema

## License & usage restrictions

Open Government Licence v3.0. Attribute DfT.

## Location

- S3: `s3://dantelore.data.incoming/naptan_stops/grid_e=<e>/grid_n=<n>/naptan_stops.parquet`
- Glue: `incoming.naptan_stops`
- Format: Parquet, partitioned by 100km OS grid tile (`grid_e`, `grid_n`, int) like the other gov-etl point datasets. Thatcham is in `grid_e = 4, grid_n = 1`.

## Field spec

Column names are the source's, snake_cased.

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| atco_code | string | No | Primary; joins to BODS stop IDs | ATCO code. The first three digits are the administrative area (`030` West Berkshire, `039` Reading) |
| naptan_code | string | Yes | | Short SMS code shown on stop flags |
| plate_code, cleardown_code | string | Yes | | Operator plate and real-time cleardown codes |
| common_name | string | Yes | | Stop name, e.g. `Thatcham Broadway` |
| short_common_name | string | Yes | | Short name |
| landmark | string | Yes | | Nearby landmark |
| street | string | Yes | | Street |
| crossing | string | Yes | | Cross street |
| indicator | string | Yes | | Position relative to the landmark or street, e.g. `opp`, `adj`, `Stop A` |
| bearing | string | Yes | | Direction buses face at the stop (`N`, `NE`, …) |
| nptg_locality_code | string | Yes | | National Gazetteer locality |
| locality_name, parent_locality_name, grand_parent_locality_name | string | Yes | | Locality, e.g. `Thatcham` |
| town, suburb | string | Yes | | Town and suburb |
| locality_centre | boolean | Yes | | True if the stop is at the locality centre |
| grid_type | string | Yes | | `UKOS` (GB grid) |
| x_coordinate | int | No | | OSGB36 easting (metres) |
| y_coordinate | int | No | | OSGB36 northing (metres) |
| longitude, latitude | double | No | | WGS84 location |
| stop_type | string | Yes | | `BCT` on-street bus stop, `BCS` bus station bay, `RSE` rail station entrance, `RLY` rail platform access, `PLT` platform, `TMU` tram/metro, `TXR` taxi rank, … |
| bus_stop_type | string | Yes | | `MKD` marked stop, `CUS` custom (unmarked), `HAR` hail and ride, `FLX` flexible zone |
| timing_status | string | Yes | | `PTP` principal timing point, `TIP`, `OTH` |
| default_wait_time | string | Yes | | |
| notes | string | Yes | | |
| `*_lang` columns | string | Yes | | Welsh-language versions of the name fields |
| administrative_area_code | string | Yes | | Administrative area (`009` = national) |
| creation_date_time, modification_date_time | timestamp | Yes | | When the record was created and last changed |
| revision_number | string | Yes | | |
| modification | string | Yes | | `new`, `revise`, `delete` |
| status | string | Yes | | `active`, `inactive` or `pending` |
| grid_e, grid_n | int | No | Partition keys | `floor(x_coordinate / 100000)`, `floor(y_coordinate / 100000)` |

## Common queries

Active bus stops within 800m (about a 10-minute walk) of a point, here Thatcham Broadway:

```sql
select atco_code, common_name, indicator, street,
       round(sqrt(power(x_coordinate - 451745, 2) + power(y_coordinate - 167377, 2))) as metres
from incoming.naptan_stops
where grid_e = 4 and grid_n = 1 and status = 'active' and stop_type in ('BCT', 'BCS')
  and sqrt(power(x_coordinate - 451745, 2) + power(y_coordinate - 167377, 2)) <= 800
order by metres
```

West Berkshire's stops: `where grid_e = 4 and grid_n = 1 and atco_code like '030%'` (1,316 stops, active and inactive).

## Source ETL / code

`github.com/dantelore/gov-etl`, `naptan_stops/naptan_stops_load.py`.

## Freshness & update cadence

NaPTAN is updated continually by local authorities. Loaded 2026-11-12; rerun the loader for a fresh copy. Each grid tile's file is overwritten.

## Running costs (estimated)

From the S3 size on 2026-11-12. Method: [gov-etl README](README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.031 GB, 54 objects | $0.001 |
| **Total** | | **$0.001** |

## Known issues & caveats

- Includes about 48,000 inactive stops; filter on `status = 'active'` for current stops.
- Straight-line distance understates walking distance, often by a third or more where paths are indirect.
- Stop locations are entered by local authorities and are occasionally tens of metres out, or on the wrong side of the road.
- A stop in NaPTAN isn't necessarily served: check the BODS timetable (`incoming.bods_gtfs_*`, `lake.bus_planned_calls`).
- Reloading overwrites each tile but never removes a tile, so a tile that empties would keep its old file (unlikely in practice).

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|
| Thatcham analysis (thatchamplan.co.uk) | Dan | Walking distance from the development site to a frequent bus |

## Status

Active.
