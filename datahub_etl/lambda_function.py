from helpers.aws import load_file_to_s3, add_glue_partition_for
from datetime import datetime
from datahub_etl.weather_etl import extract_observations_data, transform_observations_data

INPUT_FILE = "/tmp/weather_data.json"
OUTPUT_FILE = "/tmp/observations.json"
S3_INCOMING_BUCKET = "dantelore.data.incoming"
S3_RAW_BUCKET = "dantelore.data.raw"
S3_CACHE_KEY = "weather/geohash_cache.json"


def handler(event, context):
    today = datetime.today()

    extract_observations_data(INPUT_FILE, s3_bucket=S3_RAW_BUCKET, s3_cache_key=S3_CACHE_KEY)
    save_raw_data_to_s3(today)

    try:
        transform_observations_data(INPUT_FILE, OUTPUT_FILE)
    except Exception as e:
        print("Failed to transform data. Corrupt response from API?")
        print(e)
        return {"statusCode": 500}

    s3_key = f"weather/year={today.year}/month={today.month}/day={today.day}/observations.json"

    load_file_to_s3(OUTPUT_FILE, S3_INCOMING_BUCKET, s3_key)
    add_glue_partition_for(today.year, today.month, today.day)


def save_raw_data_to_s3(today):
    raw_s3_key = f"weather/{today.year}/{today.month}/{today.day}/raw.json"
    print(f"Writing raw data to: s3://{S3_RAW_BUCKET}/{raw_s3_key}")
    load_file_to_s3(INPUT_FILE, S3_RAW_BUCKET, raw_s3_key)
