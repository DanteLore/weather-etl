resource "aws_glue_catalog_table" "incoming_weather_glue_table" {
  database_name = "incoming"
  name = "weather"
  description = "Met Office weather data, raw incoming data"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL = "TRUE"
  }

  storage_descriptor {
    location = "s3://dantelore.data.incoming/weather"
    input_format = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat"

    ser_de_info {
      name = "s3-stream"
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"

      parameters = {
        "serialization.format" = 1
      }
    }

    columns {
      name = "timestamp"
      type = "string"
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
    type = "string"
  }
  partition_keys {
    name = "month"
    type = "string"
  }
  partition_keys {
    name = "day"
    type = "string"
  }
}