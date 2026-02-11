resource "aws_glue_catalog_table" "weather_stations_glue_table" {
  database_name = "lake"
  name          = "weather_stations"
  description   = "Weather station metadata - 137 UK/Ireland monitoring stations"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL = "TRUE"
  }

  storage_descriptor {
    location      = "s3://dantelore.data.lake/weather_stations/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "json-serde"
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"

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
      name = "site_country"
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
  }
}
