from helpers.aws import execute_athena_command, delete_folder_from_s3

S3_DATA_LAKE_BUCKET = "dantelore.data.lake"

WEATHER_DIR_NAME = 'weather/'
WEATHER_TABLE_SQL = '''
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
    where (year <> '2022' and month <> '7' and site_name <> 'CHIVENOR' or temperature > -5) -- exclude broken readings from Chivenor
)
where rn = 1
'''

SUMMARY_DIR_NAME = 'weather_monthly_site_summary/'
SUMMARY_TABLE_SQL = '''
insert into lake.weather_monthly_site_summary (
    site_id,
    site_name,
    lat,
    lon,
    year,
    month,
    low_temp,
    high_temp,
    median_temp
)
select 
    site_id,
    site_name,
    lat,
    lon,
    YEAR(observation_ts) as obs_year,
    MONTH(observation_ts) as obs_month,
    approx_percentile(temperature, 0.05) as low_temp,
    approx_percentile(temperature, 0.95) as high_temp,
    approx_percentile(temperature, 0.50) as median_temp
from lake.weather
group by site_id, site_name, lat, lon, YEAR(observation_ts), MONTH(observation_ts)
'''


def build_data_models(data_lake_bucket):
    # Create the core model
    delete_folder_from_s3(data_lake_bucket, WEATHER_DIR_NAME)
    execute_athena_command(sql=WEATHER_TABLE_SQL, wait_seconds=120)

    # Create the summary table
    delete_folder_from_s3(data_lake_bucket, SUMMARY_DIR_NAME)
    execute_athena_command(sql=SUMMARY_TABLE_SQL, wait_seconds=120)


def handler(event, context):
    try:
        build_data_models(S3_DATA_LAKE_BUCKET)
    except Exception as e:
        print("Failed to transform data")
        print(e)
        return {"statusCode": 500}
