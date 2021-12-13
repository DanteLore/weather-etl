from aws_helpers import load_file_to_s3, add_glue_partition_for
from datetime import datetime
from weather_etl import extract_observations_data, transform_observations_data

INPUT_FILE = "/tmp/weather_data.json"
OUTPUT_FILE = "/tmp/observations.json"
S3_BUCKET = "dantelore.data.incoming"

S3_ERROR_FILENAME = "weather/failed/{0}.json"


def handler(event, context):
    today = datetime.today()

    extract_observations_data(INPUT_FILE)

    try:
        transform_observations_data(INPUT_FILE, OUTPUT_FILE)
    except Exception as e:
        return handle_transformation_error(context, e)

    s3_key = f"weather/year={today.year}/month={today.month}/day={today.day}/observations.json"

    load_file_to_s3(OUTPUT_FILE, S3_BUCKET, s3_key)
    add_glue_partition_for(today.year, today.month, today.day)


def handle_transformation_error(context, e):
    print("Failed to transform data. Corrupt response from API?")
    print(e)
    error_dump_s3_key = S3_ERROR_FILENAME.format(context.aws_request_id)
    print("Writing API data to: " + error_dump_s3_key)
    load_file_to_s3(INPUT_FILE, S3_BUCKET, error_dump_s3_key)
    return {"statusCode": 500}


