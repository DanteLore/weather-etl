# DfT Walking and Cycling Statistics by Local Authority (CW0301–CW0303)

## Description

The share of adults (16+) who walk or cycle, by purpose and how often, every year since 2016, for England, its regions, counties and every local authority. From Sport England's Active Lives Survey, published by DfT as tables CW0301 (walking or cycling), CW0302 (cycling) and CW0303 (walking). Loaded from CW0307, which holds all three tables unrounded, with confidence intervals and sample sizes. 96,480 rows for 335 areas. The only yearly measure of cycling at local authority level: e.g. the share of West Berkshire adults who cycle at least once a week, against neighbouring areas and England.

## Classification

Public. Aggregated survey estimates; no personal data.

## Owner

Dan. See the gov-etl repo for the code.

## Source & references

DfT, "Walking and cycling statistics (CW)": https://www.gov.uk/government/statistical-data-sets/walking-and-cycling-statistics-cw (workbook `cw0307.ods`). Background and methods: https://www.gov.uk/government/collections/walking-and-cycling-statistics. Survey: Sport England Active Lives Adult Survey.

## License & usage restrictions

Open Government Licence v3.0. Attribute DfT and Sport England.

## Location

- S3: `s3://dantelore.data.incoming/dft_walking_cycling_la/dft_walking_cycling_la.parquet`
- Glue: `incoming.dft_walking_cycling_la`
- Format: Parquet, not partitioned

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| area_code | string | No | Key | ONS code: `E92000001` England, `E12…` regions, `E10…` counties, `E06…`/`E07…`/`E08…`/`E09…` local authorities |
| area_name | string | No | | E.g. `West Berkshire` |
| area_class | string | No | | `Country`, `Region`, `County`, `Unitary Authority`, `Non-metropolitan District`, `Metropolitan District`, `London Borough`, `Metropolitan County`, `Inner London`, `Outer London` |
| mode | string | No | Key | `Walking`, `Cycling` or `Walking or cycling` |
| purpose | string | No | Key | `Any`, `Leisure` (health, recreation, training) or `Travel` (getting from place to place) |
| frequency | string | No | Key | `At least once per month`, `At least once per week`, `At least 3 times per week`, `At least 5 times per week` |
| year | int | No | Key | Survey year: `2023` is mid-November 2022 to mid-November 2023 |
| pct | double | Yes | | Percentage of adults; null when suppressed |
| ci_margin_pct | double | Yes | | Margin of the confidence interval: the interval is `pct ± ci_margin_pct`. Null when suppressed, and for authorities aggregated from pre-reorganisation councils |
| sample_size | int | Yes | | Unweighted number of respondents in the category (e.g. those who cycle at least weekly), not everyone surveyed in the area |
| is_suppressed | boolean | No | | True when DfT suppressed the figure (fewer than 30 respondents) |
| edition_published | string | No | | Date the gov.uk page was last updated |

## Common queries

Weekly cycling in West Berkshire against the South East and England:

```sql
select area_name, year, round(pct, 1) as pct, ci_margin_pct
from incoming.dft_walking_cycling_la
where area_code in ('E06000037', 'E12000008', 'E92000001')
  and mode = 'Cycling' and purpose = 'Any' and frequency = 'At least once per week'
order by area_name, year
```

Cycling for travel (not leisure) at least once a month, all unitary authorities, latest year:

```sql
select area_name, round(pct, 1) as pct, ci_margin_pct
from incoming.dft_walking_cycling_la
where area_class = 'Unitary Authority' and mode = 'Cycling' and purpose = 'Travel'
  and frequency = 'At least once per month' and year = 2023 and not is_suppressed
order by pct desc
```

## Source ETL / code

`github.com/dantelore/gov-etl`, `dft_walking_cycling/dft_walking_cycling_load.py`. Reads the three sheets of CW0307 (proportions, confidence intervals, sample sizes) into one long table.

```bash
python dft_walking_cycling/dft_walking_cycling_load.py
```

## Freshness & update cadence

Manual. The edition loaded on 2026-11-12 (page last updated 2024-08-28) covers 2016 to 2023. Its cover sheet says "Next update: Summer 2025", but no newer edition was on the page on 2026-11-12. Rerun when one appears; the file is replaced.

## Running costs (estimated)

From the S3 size on 2026-11-12. Method: [gov-etl README](README.md#running-costs).

| Item | Basis | Est. $/month |
|---|---|---|
| S3 storage | 0.001 GB, 1 object | <$0.001 |
| **Total** | | **<$0.001** |

## Known issues & caveats

- **Wide margins locally.** Each authority has a few hundred respondents a year, so local figures carry margins of ±3 to ±5 points (West Berkshire weekly cycling: 9.0% ±3.2 in 2023). Year-to-year changes are mostly noise; look at multi-year trends or compare with the margin.
- 2020 and 2021 cover the pandemic (2020 also saw a leisure cycling boom).
- Results are by where respondents live, not where they walk or cycle.
- "Walking" means continuous walks of over 10 minutes; "cycling" is any cycling.
- DfT's main sheet gives some authorities the wrong class (West Berkshire as a metropolitan district); `area_class` is taken from the sample size sheet, which has it right.
- Figures for authorities created since 2016 (e.g. West Northamptonshire) are aggregated from the old councils for earlier years, without confidence intervals.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|
| Thatcham analysis (thatchamplan.co.uk) | Dan | Cycling trend in West Berkshire |

## Status

Active.
