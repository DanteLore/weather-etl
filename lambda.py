import boto3
from datetime import datetime
from weather_etl import extract_observations_data, transform_observations_data

INPUT_FILE = "/tmp/weather_data.json"
OUTPUT_FILE = "/tmp/observations.json"
S3_BUCKET = "dantelore.data.incoming"
S3_FILENAME = "weather/{year}/{month}/{day}/observations.json".format(year=datetime.now().year,
                                                                      month=datetime.now().month,
                                                                      day=datetime.now().day)
S3_ERROR_FILENAME = "weather/failed/{0}.json"


def handler(event, context):
    extract_observations_data(INPUT_FILE)

    try:
        transform_observations_data(INPUT_FILE, OUTPUT_FILE)
    except Exception as e:
        return handle_transformation_error(context, e)

    load_file_to_s3(OUTPUT_FILE, S3_BUCKET, S3_FILENAME)


def handle_transformation_error(context, e):
    print("Failed to transform data. Corrupt response from API?")
    print(e)
    error_dump_s3_key = S3_ERROR_FILENAME.format(context.aws_request_id)
    print("Writing API data to: " + error_dump_s3_key)
    load_file_to_s3(INPUT_FILE, S3_BUCKET, error_dump_s3_key)
    return {"statusCode": 500}


def load_file_to_s3(filename, s3_bucket, s3_key):
    s3 = boto3.resource('s3')
    s3.Bucket(s3_bucket).upload_file(
        filename,
        s3_key
    )
