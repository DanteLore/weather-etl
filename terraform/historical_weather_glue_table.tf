resource "aws_glue_catalog_table" "historical_weather_glue_table" {
  database_name = "incoming"
  name = "midas"
  description = "MIDAS Historical Weather Data"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL = "TRUE"
  }

  storage_descriptor {
    location = "s3://dantelore.data.incoming/midas"
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
      name = "ob_time"
      type = "timestamp"
    }
    columns {
      name = "id"
      type = "string"
    }
    columns {
      name = "id_type"
      type = "string"
    }
    columns {
      name = "met_domain_name"
      type = "string"
    }
    columns {
      name = "src_id"
      type = "int"
    }
    columns {
      name = "wind_speed_unit_id"
      type = "int"
    }
    columns {
      name = "wind_direction"
      type = "int"
    }
    columns {
      name = "wind_speed"
      type = "float"
    }
    columns {
      name = "cld_ttl_amt_id"
      type = "int"
    }
    columns {
      name = "low_cld_type_id"
      type = "int"
    }
    columns {
      name = "med_cld_type_id"
      type = "int"
    }
    columns {
      name = "hi_cld_type_id"
      type = "int"
    }
    columns {
      name = "cld_base_amt_id"
      type = "int"
    }
    columns {
      name = "cld_base_ht"
      type = "int"
    }
    columns {
      name = "visibility"
      type = "int"
    }
    columns {
      name = "msl_pressure"
      type = "float"
    }
    columns {
      name = "vert_vsby"
      type = "int"
    }
    columns {
      name = "air_temperature"
      type = "float"
    }
    columns {
      name = "dewpoint"
      type = "float"
    }
    columns {
      name = "wetb_temp"
      type = "float"
    }
    columns {
      name = "rltv_hum"
      type = "float"
    }
    columns {
      name = "stn_pres"
      type = "float"
    }
    columns {
      name = "alt_pres"
      type = "float"
    }
    columns {
      name = "ground_state_id"
      type = "string"
    }
    columns {
      name = "q10mnt_mxgst_spd"
      type = "int"
    }
    columns {
      name = "cavok_flag"
      type = "string"
    }
    columns {
      name = "cs_hr_sun_dur"
      type = "float"
    }
    columns {
      name = "wmo_hr_sun_dur"
      type = "float"
    }
    columns {
      name = "snow_depth"
      type = "int"
    }
    columns {
      name = "wind_direction_q"
      type = "int"
    }
    columns {
      name = "wind_speed_q"
      type = "int"
    }
    columns {
      name = "meto_stmp_time"
      type = "string"
    }
    columns {
      name = "drv_hr_sun_dur"
      type = "float"
    }
    columns {
      name = "xxx"
      type = "string"
    }
  }

  partition_keys {
    name = "year"
    type = "string"
  }
}