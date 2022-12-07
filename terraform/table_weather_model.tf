resource "aws_glue_catalog_table" "modelled_weather_glue_table" {
  database_name = "lake"
  name = "weather"
  description = "Met Office weather data, deduped and cleaned"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL = "TRUE"
  }

  storage_descriptor {
    location = "s3://dantelore.data.lake/weather"
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
      name = "observation_ts"
      type = "timestamp"
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
      name = "site_country"
      type = "string"
    }

    columns {
      name = "site_continent"
      type = "string"
    }

    columns {
      name = "site_elevation"
      type = "double"
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
      name = "wind_direction"
      type = "string"
    }

    columns {
      name = "screen_relative_humidity"
      type = "double"
    }

    columns {
      name = "pressure"
      type = "double"
    }

    columns {
      name = "wind_speed"
      type = "double"
    }

    columns {
      name = "temperature"
      type = "double"
    }

    columns {
      name = "visibility"
      type = "int"
    }

    columns {
      name = "weather_type"
      type = "int"
    }

    columns {
      name = "pressure_tendency"
      type = "string"
    }

    columns {
      name = "dew_point"
      type = "double"
    }
  }

  partition_keys {
    name = "year"
    type = "bigint"
  }
  partition_keys {
    name = "month"
    type = "bigint"
  }
}