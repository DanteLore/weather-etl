from helpers.aws import execute_athena_command
from datetime import datetime

S3_INCOMING_BUCKET = "dantelore.data.incoming"
S3_DATA_LAKE_BUCKET = "dantelore.data.lake"


SQL = '''
insert into lake.weather
select 
    observation_ts,    
    site_id,             
    site_name,           
    site_country,        
    site_continent,      
    site_elevation,      
    lat,                 
    lon,                 
    wind_direction,      	
    screen_relative_humidity,
    pressure,            	
    wind_speed,          	
    temperature,         	
    visibility,          	
    weather_type,
    pressure_tendency,
    dew_point, 
    obs_year as year,
    obs_month as month	   
from 
( 
    select 
    observation_ts,    
    site_id,             
    site_name,           
    site_country,        
    site_continent,      
    site_elevation,      
    lat,                 
    lon,                 
    wind_direction,      	
    screen_relative_humidity,
    pressure,            	
    wind_speed,          	
    temperature,         	
    visibility,          	
    weather_type,
    pressure_tendency,
    dew_point, 
    month(observation_ts) as obs_month,
    year(observation_ts) as obs_year,
    ROW_NUMBER() OVER ( PARTITION BY date_trunc('hour', observation_ts), site_id ORDER BY observation_ts DESC ) as rn
    from weather
)
where rn = 1
'''


def build_data_models(incoming_bucket, data_lake_bucket):
    print("Modelled!")


def handler(event, context):
    try:
        build_data_models(S3_INCOMING_BUCKET, S3_DATA_LAKE_BUCKET)
    except Exception as e:
        print("Failed to transform data")
        print(e)
        return {"statusCode": 500}
