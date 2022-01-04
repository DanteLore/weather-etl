from aws_helpers import add_glue_partition_for, download_file_from_s3, load_file_to_s3
from datetime import datetime
from weather_etl import extract_observations_data, transform_observations_data, validate_json

YEAR = 2022
MONTH = 1
DAY = 2

S3_INCOMING_BUCKET = "dantelore.data.incoming"
S3_RAW_BUCKET = "dantelore.data.raw"
RAW_FILE = "weatherData/temp.raw.json"
TRANSFORMED_FILE = "weatherData/temp.transformed.json"

if __name__ == '__main__':
    print("Backfilling...")

    raw_s3_key = f"weather/{YEAR}/{MONTH}/{DAY}/raw.json"
    download_file_from_s3(S3_RAW_BUCKET, raw_s3_key, RAW_FILE)
    transform_observations_data(RAW_FILE, TRANSFORMED_FILE)
    s3_key = f"weather/year={YEAR}/month={MONTH}/day={DAY}/observations.json"
    load_file_to_s3(TRANSFORMED_FILE, S3_INCOMING_BUCKET, s3_key)
    add_glue_partition_for(YEAR, MONTH, DAY)
