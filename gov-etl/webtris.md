# National Highways WebTRIS Traffic Counts (M4 J12–J14 and A34)

## Description

Traffic counts and speeds from National Highways' WebTRIS (Traffic Information System) for the strategic roads around Newbury and Thatcham: the M4 from junction 12 (Theale) to junction 14 (Hungerford), and the A34 from Chieveley (M4 J13) south to the A343. Every 15 minutes, each loop detector site gives the number of vehicles by length band (cars, vans, lorries), by speed band where measured, the average speed and the total volume. 58 of the 62 active sites have data, as far back as the API holds (2017 for most): 13.5 million rows from 2017-01-01 to 2026-06-30 on 2026-11-12. Also known as MIDAS (motorway loop detectors), TAME and TMU sites. A second table lists every WebTRIS site in England.

| Table | One row per | Coverage |
|---|---|---|
| `incoming.webtris_counts` | site per 15 minutes | The M4/A34 sites near Newbury |
| `incoming.webtris_daily_flows` | site per day | Every WebTRIS site in England, active or not |
| `incoming.webtris_hourly_profiles` | site, month, weekday and hour | Every WebTRIS site in England |
| `incoming.webtris_sites` | WebTRIS site | All 20,076 |

The national tables come from WebTRIS's Monthly report: the daily flow and share of large vehicles, and the average flow in each hour for each weekday of each month. They hold 26.4 million daily rows and 147 million hourly-profile rows for 12,866 sites with data, 2017-01-01 to 2026-06-30, on 2026-11-13. The 15-minute data is local only, because loading it for every site would mean about 1.5 TB of API downloads.

## Classification

Public. Open government data; no personal data.

## Owner

Dan. See the gov-etl repo for the code.

## Source & references

National Highways WebTRIS API v1.0: https://webtris.nationalhighways.co.uk/api/swagger/ui/index. `/sites` for the site list, `/reports/Daily` for 15-minute rows, `/reports/Monthly` for the national daily flows and hourly profiles.

## License & usage restrictions

Open Government Licence v3.0. Attribute National Highways.

## Location

- S3: `s3://dantelore.data.incoming/webtris/counts/year=<yyyy>/webtris_counts_<yyyy>_site_<site_id>.parquet`, `webtris/daily_flows/year=<yyyy>/webtris_daily_flows_<yyyy>.parquet`, `webtris/hourly_profiles/year=<yyyy>/webtris_hourly_profiles_<yyyy>.parquet` and `webtris/sites/webtris_sites.parquet`
- Glue: `incoming.webtris_counts`, `incoming.webtris_daily_flows`, `incoming.webtris_hourly_profiles` (partitioned by `year`), `incoming.webtris_sites` (not partitioned)

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

### webtris_daily_flows

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| site_id | string | No | Key | WebTRIS site ID |
| report_date | date | No | Key | Day |
| day_name | string | No | | `Mon`, `Tue`, … (WebTRIS writes Thursday as `Thur`) |
| flow | int | Yes | | Vehicles that day (24 hours); null if the detector didn't report |
| large_vehicle_pct | double | Yes | | Share of vehicles over 6.6 m long |
| year | string | No | Partition key | Year |

### webtris_hourly_profiles

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| site_id | string | No | Key | WebTRIS site ID |
| month_start | date | No | Key | First day of the month |
| day_name | string | No | Key | Weekday averaged |
| hour_start | int | No | Key | Hour, `0` (00:00–01:00) to `23` |
| average_vehicles | int | Yes | | Average vehicles in that hour on that weekday that month |
| days_averaged | double | Yes | | How many days went into the average |
| average_flow_24h | int | Yes | | Average daily flow on that weekday that month |
| large_vehicle_pct_24h | double | Yes | | Share of large vehicles on that weekday that month |
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

Busiest morning peak hour on weekdays at every site in a region, nationally (join `webtris_sites` for location):

```sql
select h.site_id, s.site_name, max(h.average_vehicles) as peak_hour_vehicles
from incoming.webtris_hourly_profiles h
join incoming.webtris_sites s on s.site_id = h.site_id
where h.year = '2025' and h.day_name not in ('Sat', 'Sun') and h.hour_start between 7 and 9
  and s.latitude between 51.3 and 51.6 and s.longitude between -1.5 and -0.9
group by 1, 2 order by 3 desc limit 20
```

## Source ETL / code

`github.com/dantelore/gov-etl`, `webtris/webtris_load.py`. Reloads the site list, then fetches a year at a time for each active site in the area (latitude 51.36–51.50, longitude −1.40 to −1.05), half a second apart. A site-year with no data is skipped.

```bash
python webtris/webtris_load.py                   # everything since 2010 (data starts in 2017)
python webtris/webtris_load.py --from-year 2026  # refresh the current year
python webtris/webtris_load.py --national        # daily flows and hourly profiles for every site (about 800 requests a year)
```

The national load asks for 25 sites per request, half a second apart, and writes one file per year per table. WebTRIS's errors (500s) are retried with growing pauses; if a batch keeps failing, its sites are fetched one at a time and any site that still fails is skipped and logged. In the 2026-11-12 load that skipped one site, 6506, for 2018 only.

## Freshness & update cadence

Manual. WebTRIS publishes data a few months in arrears: on 2026-11-12 the latest was 2026-06-30. Rerun with `--from-year` for the current year to add newer months; each site-year file is replaced.

## Running costs (estimated)

From the S3 size on 2026-11-12. Method: [gov-etl README](README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage, 15-minute counts and sites | 0.072 GB, 412 objects (13.5 million rows) | $0.002 |
| S3 storage, national daily flows and hourly profiles | 0.373 GB, 20 objects | $0.009 |
| **Total** | | **$0.010** |

Querying it: a year of all sites is about 1 MB per column read.

## Known issues & caveats

- **The 15-minute counts cover active sites only**: the 62 near Newbury active in November 2026, so sites retired earlier (including many older M4 MIDAS loops) have no 15-minute history here. The national daily and hourly tables include inactive sites.
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
