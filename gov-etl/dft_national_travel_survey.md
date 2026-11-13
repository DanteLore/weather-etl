# DfT National Travel Survey and National Travel Attitudes Study Tables

## Description

How people in England travel and what they think about it, from DfT's two national surveys. Published tables only; national (and regional / rural-urban) figures, nothing local.

| Table | Contents |
|---|---|
| `incoming.dft_nts_tables` | Every National Travel Survey data table (78 workbooks: NTS0101 to NTS9920, plus the combined authority estimates `nts…_ca`), in one long format: trips, distance and time by mode, purpose, trip length, age, sex, income, car access, region, rural-urban class, school travel, cycle ownership and more. Mostly 2002 to 2025, a few series from as early as 1951. 786,400 rows |
| `incoming.dft_ntas_responses` | Every question in National Travel Attitudes Study waves 7 to 10 (2022 to 2024), with response percentages and sample sizes, e.g. wave 9's reasons people don't cycle more (safety, weather, distance, hills, cost) and what would encourage them (safer roads, segregated paths, secure storage) |

## Classification

Public. Aggregated survey estimates; no personal data.

## Owner

Dan. See the gov-etl repo for the code.

## Source & references

- NTS data tables: https://www.gov.uk/government/collections/national-travel-survey-statistics (pages "nts01" to "nts09", "nts99" and "National Travel Survey combined authority estimates"). Notes and definitions: the NTS technical reports and each workbook's Notes sheet.
- NTAS: https://www.gov.uk/government/collections/national-travel-attitudes-study-ntas (waves 7 to 10, one `.ods` each).

## License & usage restrictions

Open Government Licence v3.0. Attribute DfT.

## Location

- S3: `s3://dantelore.data.incoming/dft_nts_tables/table_id=<id>/dft_nts_tables_<id>.parquet`, `s3://dantelore.data.incoming/dft_ntas_responses/wave=<n>/dft_ntas_responses_<n>.parquet`
- Glue: `incoming.dft_nts_tables` (partitioned by `table_id`, e.g. `nts0308`, `nts0303_ca`), `incoming.dft_ntas_responses` (partitioned by `wave`, `7` to `10`)

## Field spec

### dft_nts_tables

Each workbook sheet is a table with a year column, up to three category columns and one or more measure columns; each measure cell is a row here.

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| sheet | string | No | Key | Sheet, e.g. `NTS0308a_trips`, `NTS0308c_distance` |
| sheet_title | string | No | | The sheet's title, e.g. `NTS0308a: Average number of trips by trip length and main mode: England, 2002 onwards` |
| year_label | string | No | | Year as published |
| year | int | No | Key | Year |
| dimension_1_name, dimension_1 | string | Yes | Key | First category column and value, e.g. `Main mode`, `Pedal cycle` |
| dimension_2_name, dimension_2, dimension_3_name, dimension_3 | string | Yes | Key | Further category columns where the table has them |
| measure | string | No | Key | The column heading, e.g. `Under 1 mile`, `All lengths`, `Commuting` |
| value | double | Yes | | The figure (units as in `sheet_title`: trips per person per year, miles, %, …) |
| value_marker | string | Yes | | Text in place of a number: `[u]` (112,469 cells; too few respondents to publish, see each workbook's Notes sheet) or `[x]` (not available) |
| sample_size | double | Yes | | The row's unweighted sample size column, if the sheet has one |
| sample_size_label | string | Yes | | Its heading, which gives the unit (e.g. `Unweighted sample size: trips (thousands)`) |
| edition_published | string | No | | Date the gov.uk page was last updated |
| table_id | string | No | Partition key | Workbook, e.g. `nts0308`; `_ca` for combined authority estimates |

Note markers (`[note 3]`) are stripped from labels.

### dft_ntas_responses

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| sheet, sheet_title | string | No | | E.g. `NTAS_9d`, `What barriers or factors discourage people from cycling more` |
| question | string | No | Key | Full question text; for multi-part questions the part follows `?: `, e.g. `…which of these apply to you?: It’s too hilly in my area` |
| question_base | string | Yes | | Who was asked, e.g. `All respondents` |
| response | string | Yes | Key | E.g. `Yes`, `Strongly agree` |
| comparison_variable, comparison_value | string | Yes | | Breakdowns where published (waves 8 to 10), e.g. sex or age band; null for the all-respondents figure |
| pct | double | Yes | | Percentage giving the response |
| value_marker | string | Yes | | Text in place of a percentage: `[u]`, `[z]` or `[low]`, as defined in each wave's Notes sheet |
| sample_size_response, sample_size_base | int | Yes | | Unweighted respondents giving the response / asked the question |
| notes, variable_name | string | Yes | | As published (variable names in waves 9 and 10 only) |
| edition_published | string | No | | Date the release page was last updated |
| wave | string | No | Partition key | `7` (2022) to `10` (2024) |

## Common queries

Trips per person per year by trip length, walking and cycling, England (NTS0308):

```sql
select year, dimension_1 as mode, measure as trip_length, round(value, 1) as trips_per_person
from incoming.dft_nts_tables
where table_id = 'nts0308' and sheet = 'NTS0308a_trips' and dimension_1 in ('Walk', 'Pedal cycle')
  and year = (select max(year) from incoming.dft_nts_tables where table_id = 'nts0308')
order by 2, 3
```

Why people don't cycle more (NTAS wave 9, 2023):

```sql
select split_part(question, '?: ', 2) as reason, round(pct, 1) as pct, sample_size_base
from incoming.dft_ntas_responses
where wave = '9' and sheet = 'NTAS_9d' and response = 'Yes' and comparison_variable is null
order by pct desc
```

Find a table: `select distinct table_id, sheet, sheet_title from incoming.dft_nts_tables where lower(sheet_title) like '%cycl%'`.

## Source ETL / code

`github.com/dantelore/gov-etl`, `dft_national_travel_survey/dft_national_travel_survey_load.py`. Reads every `.ods` on the NTS pages with a generic parser (header row starting `Year`; text columns before the first numeric one are categories), and the question sheets of each NTAS wave.

```bash
python dft_national_travel_survey/dft_national_travel_survey_load.py
python dft_national_travel_survey/dft_national_travel_survey_load.py --table dft_ntas_responses
```

## Freshness & update cadence

Manual. NTS is published each September (the edition loaded on 2026-11-12, published 2026-09-10, runs to 2025) and revises earlier years, so rerun after each release; each table's file is replaced. NTAS waves are one-off releases; add new waves to `NTAS_WAVES` in the loader.

## Running costs (estimated)

From the S3 size on 2026-11-12. Method: [gov-etl README](README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.007 GB, 82 objects | <$0.001 |
| **Total** | | **<$0.001** |

## Known issues & caveats

- **England only, no local figures.** Use the region (`nts99…`, `nts0318`, `nts0413`) and rural-urban (`nts0319`, `nts0414`, `nts9920`) tables for the closest match to an area like Thatcham, and the combined authority tables for big cities.
- Units differ by sheet; always read `sheet_title`. Many tables give trips per person per year; some give percentages.
- 2020 and 2021 are pandemic years, with smaller samples; DfT flags breaks in series in each workbook's Notes sheet.
- The generic parser keeps whatever headings DfT uses, so the same idea can be named differently in different tables (e.g. `Main mode` vs `Mode`).
- **NTAS waves 1 to 6 (2019 to 2021) aren't loaded**: each uses a different one-off layout. Wave 5 (early 2021) also asked about cycling barriers and encouragements.
- NTAS is an online panel survey of a few thousand adults per wave; percentages are rounded estimates with sampling error. Questions vary by wave, so trends are only possible where a question was repeated.
- The survey microdata (individual trips and responses) is only available from the UK Data Service under licence and isn't loaded.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|
| Thatcham analysis (thatchamplan.co.uk) | Dan | Trip lengths by purpose; reasons for not cycling |

## Status

Active.
