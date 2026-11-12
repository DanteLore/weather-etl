# National Highways WebTRIS Traffic Counts (M4 J12–J14 and A34)

## Description

Traffic counts and speeds from National Highways' WebTRIS (Traffic Information System) for the strategic roads around Newbury and Thatcham: the M4 from junction 12 (Theale) to junction 14 (Hungerford), and the A34 from Chieveley (M4 J13) south to the A343. Every 15 minutes, each loop detector site gives the number of vehicles by length band (cars, vans, lorries), by speed band where measured, the average speed and the total volume. 58 of the 62 active sites have data, as far back as the API holds (2017 for most): 13.5 million rows from 2017-01-01 to 2026-06-30 on 2026-11-12. Also known as MIDAS (motorway loop detectors), TAME and TMU sites. A second table lists every WebTRIS site in England.

| Table | One row per |
|---|---|
| `incoming.webtris_counts` | site per 15 minutes |
| `incoming.webtris_sites` | WebTRIS site (20,076, all England) |

## Classification

Public. Open government data; no personal data.

## Owner

Dan. See the gov-etl repo for the code.

## Source & references

National Highways WebTRIS API v1.0: https://webtris.nationalhighways.co.uk/api/swagger/ui/index. `/sites` for the site list, `/reports/Daily` for 15-minute rows.

## License & usage restrictions

Open Government Licence v3.0. Attribute National Highways.

## Location

- S3: `s3://dantelore.data.incoming/webtris/counts/year=<yyyy>/webtris_counts_<yyyy>_site_<site_id>.parquet` and `s3://dantelore.data.incoming/webtris/sites/webtris_sites.parquet`
- Glue: `incoming.webtris_counts` (partitioned by `year`), `incoming.webtris_sites` (not partitioned)

## Field spec

### webtris_counts

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| site_id | string | No | Key; joins to webtris_sites | WebTRIS site ID |
| site_name | string | No | | Site name, e.g. `M4/2853A`, `30360306`, `5614/1` |
| report_date | date | No | Key | Day |
| time_period_ending | string | No | | End of the 15-minute period, `HH:MM:SS` (e.g. `07:14:00` for 07:00–07:15) |
| time_interval | int | No | Key | Period of the day, `0` (00:00–00:15) to `95` |
| vehicles_0_520cm | int | Yes | | Vehicles up to 5.2 m long (cars, small vans) |
| vehicles_521_660cm | int | Yes | | 5.21–6.6 m (large vans) |
| vehicles_661_1160cm | int | Yes | | 6.61–11.6 m (rigid lorries, buses) |
| vehicles_over_1160cm | int | Yes | | Over 11.6 m (articulated lorries) |
| vehicles_0_10mph … vehicles_over_80mph | int | Yes | | Vehicles by speed band (14 bands); blank at many sites |
| avg_mph | int | Yes | | Average speed |
| total_volume | int | Yes | | All vehicles in the 15 minutes; null when the detector reported nothing |
| year | string | No | Partition key | Year |

### webtris_sites

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| site_id | string | No | Primary | WebTRIS site ID |
| site_name | string | No | | E.g. `M4/2853A` (MIDAS: road, distance marker in 100 m, carriageway `A` westbound / `B` eastbound, `J`/`K`/`L`/`M` slip roads) |
| description | string | Yes | | Site type, link, grid reference and direction, e.g. `MIDAS site at M4/2853A priority 1 on link 105003702; GPS Ref: 454029;174063; Westbound` |
| longitude, latitude | double | No | | Location |
| status | string | No | | `Active` or `Inactive` |

## Common queries

Average weekday traffic on the M4 between J13 and J12 by hour, both directions (sites `M4/2826B` eastbound and `M4/2828A` westbound):

```sql
select s.site_name, hour(date_parse(c.time_period_ending, '%H:%i:%s')) as hour,
       round(avg(c.total_volume) * 4) as vehicles_per_hour, round(avg(c.avg_mph)) as avg_mph
from incoming.webtris_counts c
join incoming.webtris_sites s on s.site_id = c.site_id
where c.year = '2025' and s.site_name in ('M4/2826B', 'M4/2828A')
  and day_of_week(c.report_date) between 1 and 5
group by 1, 2 order by 1, 2
```

Daily traffic on the A34 at the A4 junction, year by year:

```sql
select c.year, s.site_name, round(sum(c.total_volume) / count(distinct c.report_date)) as vehicles_per_day
from incoming.webtris_counts c
join incoming.webtris_sites s on s.site_id = c.site_id
where s.site_name like '5614/%' or s.site_name like '5615/%'
group by 1, 2 order by 2, 1
```

## Source ETL / code

`github.com/dantelore/gov-etl`, `webtris/webtris_load.py`. Reloads the site list, then fetches a year at a time for each active site in the area (latitude 51.36–51.50, longitude −1.40 to −1.05), half a second apart. A site-year with no data is skipped.

```bash
python webtris/webtris_load.py                   # everything since 2010 (data starts in 2017)
python webtris/webtris_load.py --from-year 2026  # refresh the current year
```

## Freshness & update cadence

Manual. WebTRIS publishes data a few months in arrears: on 2026-11-12 the latest was 2026-06-30. Rerun with `--from-year` for the current year to add newer months; each site-year file is replaced.

## Running costs (estimated)

From the S3 size on 2026-11-12. Method: [gov-etl README](README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.072 GB, 412 objects (13.5 million rows) | $0.002 |
| **Total** | | **$0.002** |

Querying it: a year of all sites is about 1 MB per column read.

## Known issues & caveats

- **Inactive sites aren't loaded.** Only the 62 sites active in November 2026; sites retired earlier (including many older M4 MIDAS loops) have history that isn't here.
- **Gaps.** Detectors fail; a null `total_volume` means no report for that period, and some site-years have months missing. Check coverage before comparing totals.
- Speed bands are blank at most sites; `avg_mph` is more widely filled.
- MIDAS sites on the motorway main carriageway count one direction each (`A`/`B` in the name). Slip-road (`J`/`K`/`L`/`M`) and TMU/TAME sites count individual links, so don't add sites together without checking which links they cover.
- 2020 and 2021 are depressed by the pandemic.
- The API only goes back to about 2017 for these sites, although some were installed much earlier.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|
| Thatcham analysis (thatchamplan.co.uk) | Dan | M4 and A34 traffic near Thatcham |

## Status

Active.
