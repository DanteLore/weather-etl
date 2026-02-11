# Weather ETL Project

## Overview
ETL pipeline that extracts weather observation data from the UK Met Office DataHub API, transforms it, and loads it into AWS data lake for analysis.

Running successfully for over a year, this project demonstrates that solid data engineering principles apply at any scale. It prioritizes development speed over infrastructure complexity using serverless AWS tools.

**Project Philosophy**: [World's Simplest Data Pipeline](https://dantelore.com/posts/simplest-data-pipeline/) | [Data Modeling Approach](https://dantelore.com/posts/simplest-data-model/)

## Five Golden Rules

These principles guide all design decisions:

1. **Idempotent operations** - Processes are deterministic and repeatable. Same inputs → same outputs, always.
2. **Immutable data** - Write-only, no updates or deletions. Raw source data preserved for recovery.
3. **Simple left-to-right flow** - Clear separation: Extract → Transform → Load. No circular dependencies.
4. **Schema on write** - Validate during writes, not reads. Fail fast on bad data.
5. **Fail fast** - Let errors surface naturally. Don't mask issues with excessive error handling.

## Data Engineering Principles

### Testing Philosophy
**Critical insight**: "The behaviour of the system is dependent on both the code you deploy and the data it interacts with."

- Traditional unit testing is insufficient for data systems
- Test with real data, evaluate output quality
- Visualize and monitor data health rather than trying to fix everything internally
- Let users make informed decisions about data reliability

### Data Quality Strategy
- Store raw source data in S3 - enables recovery when processing logic fails
- Deduplication using window functions (partitioned by hour + site_id)
- Manual anomaly identification through visualization
- Filter problematic records in SQL rather than hiding them
- Overnight full-table rebuilds ensure changes propagate across entire dataset

### Documentation Over Abstraction
- Make business logic explicit in field names
- `temp_at_95th_percentile` not `high_temp`
- Prevent user misinterpretation through clear naming
- Code should document intent, not hide complexity

## Architecture

### Data Flow
1. **Extract**: Fetch weather observations from 135 UK/Ireland weather stations via DataHub API
2. **Transform**: Convert observations to standardized format with site metadata
3. **Load**: Store as Parquet in S3, queryable via Athena

### AWS Services
- **Lambda**: Serverless ETL execution (2 functions)
- **S3**: Data lake storage (`dantelore.data.lake`, `dantelore.queryresults`)
- **Glue**: Data catalog and table schemas
- **Athena**: SQL queries on data lake
- **Terraform**: Infrastructure as code

### Key Components
- `datahub_etl/` - Main ETL code for DataHub API
- `weather_data_model/` - Post-processing lambda
- `helpers/aws.py` - All AWS/boto3 operations (centralized)
- `terraform/` - Infrastructure definitions

## Coding Preferences

### Style
- **Short, focused methods** - Each function does one thing clearly
- **No unnecessary comments** - Code should be self-explanatory
- **Fail fast** - Avoid excessive error handling/guards, let exceptions bubble up naturally
- **No docstrings on obvious functions** - Save them for complex public APIs only

### AWS Operations
**CRITICAL**: All AWS/boto3 operations MUST go through `helpers/aws.py`
- Never import boto3 directly in application code
- Add new helper functions to `helpers/aws.py` if needed
- Keep AWS logic centralized for testing and maintenance

### File Organization
- Use helper functions (`_prefixed_names`) to break down complex logic
- Keep main functions clean and readable
- Prefer multiple small functions over one large function with comments

## Project Structure

### Lambda Functions

#### Weather ETL (`datahub_etl/`)
- `lambda_function.py` - Entry point, orchestrates ETL
- `weather_etl.py` - Core ETL logic (extract, transform)
- `site_loader.py` - Weather station metadata loading (Athena/JSON fallback)
- `datahub_client.py` - DataHub API client
- `sites.json` - 135 weather station definitions (source of truth)

#### Weather Data Model (`weather_data_model/`)
- Post-processing and deduplication

### Data Sources

#### Weather Stations
- **Primary**: Athena query on `lake.weather_stations` table (when `USE_ATHENA_SITES=true`)
- **Fallback**: Local `sites.json` file (default, works offline)
- **Storage**: S3 at `s3://dantelore.data.lake/weather_stations/sites.json`
- **Count**: 135 UK/Ireland Met Office stations

#### Geohash Cache
- Maps site_id → geohash for API efficiency
- Stored in `geohash_cache.json` and S3
- Built incrementally during ETL runs

### Data Lake Schema

#### Table: `lake.weather_stations`
```sql
site_id         string
site_name       string
site_country    string
site_elevation  double
lat             double
lon             double
```

#### Table: `lake.weather` (partitioned by year/month)
Weather observations with site metadata

## Testing

### Framework
- **pytest** with `unittest.mock`
- Centralized fixtures in `tests/fixtures.py`
- 19 tests covering ETL pipeline and site loading

### Patterns
- Use `@patch` to mock AWS helper functions (e.g., `@patch('helpers.aws.execute_athena_query')`)
- Use `@patch.dict(os.environ, {...})` for environment variable tests
- Prefer dependency injection for clients (pass mock objects)
- Use `tempfile` for isolated file I/O testing
- Clear caches in `setup_method()` for isolation

### Running Tests
```bash
pytest tests/ -v                    # All tests
pytest tests/test_site_loader.py -v # Specific module
```

## Deployment

### Build & Deploy
```bash
bash build.sh
```

This script:
1. Packages Lambda functions with dependencies
2. Uploads `sites.json` to S3
3. Runs `terraform apply` to update infrastructure

### Environment Variables

#### Lambda: `load_weather_data`
- `USE_ATHENA_SITES` - Set to `'true'` to load sites from Athena (default: `'false'`, uses local JSON)

## Git

### Ignored Files
- `api_key.py` - API credentials (never commit)
- `*.zip` - Lambda deployment packages
- `.terraform*`, `terraform.tfstate*` - Terraform state

### Committed Files
- `sites.json` - Weather station metadata (source of truth)
- `geohash_cache.json` - Geohash lookup cache

## Common Tasks

### Update Weather Station List
1. Edit `datahub_etl/sites.json`
2. Run `bash build.sh` to deploy
3. Sites automatically sync to S3 and Glue table

### Add New AWS Helper
1. Add function to `helpers/aws.py` (follow existing patterns)
2. Update application code to use new helper
3. Add tests mocking the new helper

### Refactor Code
- Keep functions short and focused
- Extract helper functions for complex logic
- Remove comments that just repeat what the code does
- Let code fail fast - don't add guards unless truly needed

## Notes
- The project previously used Datapoint API (now deprecated) - legacy code in `datapoint_etl/`
- Module-level caching in `site_loader.py` persists across Lambda invocations (warm starts)
- Glue table uses JsonSerDe for array-format JSON files
