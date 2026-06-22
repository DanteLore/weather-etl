# weather-etl

Datasets produced by the [weather-etl](https://github.com/dantelore/weather-etl) pipeline. Covers UK and global meteorological observations from two sources: the Met Office DataHub API (live hourly feed) and the CEDA MIDAS Open archive (quality-controlled historic data).

All datasets land in S3 under `s3://dantelore.data.incoming/` and are queryable via Athena (`incoming` database).

## Datasets

| Dataset | Description |
|---|---|
| [datapoint_etl](datapoint_etl.md) | Hourly weather observations from Met Office stations via the DataHub API |
| [ceda_midas](ceda_midas.md) | Historic quality-controlled UK hourly observations from the CEDA MIDAS Open archive |
