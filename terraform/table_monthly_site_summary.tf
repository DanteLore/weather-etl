resource "aws_glue_catalog_table" "weather_monthly_site_summary_glue_table" {
  database_name = "lake"
  name = "weather_monthly_site_summary"
  description = "Summarised temperature data by month by site"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL = "TRUE"
  }

  storage_descriptor {
    location = "s3://dantelore.data.lake/weather_monthly_site_summary"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet-stream"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

      parameters = {
        "serialization.format" = 1
      }
    }

    columns {
      name = "site_id"
      type = "string"
    }

    columns {
      name = "site_name"
      type = "string"
    }

    columns {
      name = "lat"
      type = "double"
    }

    columns {
      name = "lon"
      type = "double"
    }

    columns {
      name = "year"
      type = "bigint"
    }

    columns {
      name = "month"
      type = "bigint"
    }

    columns {
      name = "low_temp"
      type = "double"
    }

    columns {
      name = "high_temp"
      type = "double"
    }

    columns {
      name = "median_temp"
      type = "double"
    }
  }
}