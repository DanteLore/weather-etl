# carbon_intensity_generation

## Description

National UK electricity generation mix by fuel type, expressed as percentage shares, half-hourly. Sourced from the Carbon Intensity API alongside the national intensity data. Use this to answer questions like "what share of UK electricity came from wind at a given time?" For absolute MW figures by fuel type, prefer `incoming.elexon_fuelhh` which comes directly from Elexon's metered data.

## Classification

Public. The Carbon Intensity API is openly licensed with no authentication or usage restrictions.

## Owner

Dan (energy-etl). Query via `incoming.carbon_intensity_generation` in Athena.

## Source & references

- Carbon Intensity API: https://api.carbonintensity.org.uk
- Same feed as `carbon_intensity_national` — the generation mix is returned alongside the intensity figures.

## License & usage restrictions

Open data, Creative Commons. No restrictions on use.

## Location

- S3: `s3://dantelore.data.incoming/carbon_intensity/generation/`
- Glue / Athena: `incoming.carbon_intensity_generation`
- Format: NDJSON (JsonSerDe), partitioned by year/month/day

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| period_from | string | No | Primary | ISO 8601 UTC start of the half-hour period |
| period_to | string | No | | ISO 8601 UTC end of the half-hour period |
| pct_gas | double | Yes | | Gas generation share (%) |
| pct_coal | double | Yes | | Coal generation share (%) |
| pct_wind | double | Yes | | Wind generation share (%) — includes embedded/distributed generation |
| pct_solar | double | Yes | | Solar generation share (%) — includes embedded/distributed generation |
| pct_nuclear | double | Yes | | Nuclear generation share (%) |
| pct_hydro | double | Yes | | Hydro generation share (%) |
| pct_biomass | double | Yes | | Biomass generation share (%) |
| pct_imports | double | Yes | | Net imports share (%) — all interconnectors combined |
| pct_other | double | Yes | | Other generation share (%) |
| year | string | No | Partition | Unpadded year |
| month | string | No | Partition | Unpadded month |
| day | string | No | Partition | Unpadded day |

## Source ETL / code

`energy-etl` repo, `carbon_intensity_etl/`. Written by the same Lambda invocation as `carbon_intensity_national` and `carbon_intensity_regional`.

## Freshness & update cadence

Written every 30 minutes. One row per period.

## Known issues & caveats

- Percentage figures include ESO estimates for embedded (behind-the-meter) wind and solar (rooftop solar, small wind farms below the transmission threshold). `elexon_fuelhh` covers metered transmission-connected plant only — wind and solar will be understated there, particularly in summer. The two datasets are **not directly comparable**.
- **Do not average regional carbon intensity percentages** to get a national figure — regions are different sizes and an unweighted average gives wrong results. This table (`carbon_intensity_generation`) is already the correct national aggregate.
- Percentages may not sum to exactly 100 due to rounding.
- Partition columns are unpadded strings. Use `DATE_PARSE(year || '-' || month || '-' || day, '%Y-%c-%e')` in Athena.
- For absolute MW generation figures with individual interconnector breakdown, use `incoming.elexon_fuelhh`.

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|

## Status

Active.
