import boto3
from time import sleep
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


def load_file_to_s3(filename, s3_bucket, s3_key):
    print("Uploading data to S3://{0}/{1}".format(s3_bucket, s3_key))

    s3 = boto3.resource('s3')
    s3.Bucket(s3_bucket).upload_file(
        filename,
        s3_key
    )


def add_glue_partition_for(year, month, day):
    athena = boto3.client('athena')

    sql = f"ALTER TABLE weather ADD IF NOT EXISTS PARTITION (year='{year}', month='{month}', day='{day}')"
    print("Executing: " + sql)

    query_start = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={
            'Database': "incoming"
        },
        ResultConfiguration={
            'OutputLocation': "s3://dantelore.queryresults/Unsaved/"
        }
    )

    for count in range(10):
        query_execution = athena.get_query_execution(QueryExecutionId=query_start['QueryExecutionId'])
        state = query_execution.get('QueryExecution', {}).get('Status', {}).get('State')

        if state == 'FAILED':
            print("Query failed : " + query_execution.get('QueryExecution', {}).get('Status', {}).get('StateChangeReason'))
            break
        elif state == 'SUCCEEDED':
            print('Query succeeded')
            break

        print(f"Wait count {count}/10")
        sleep(1)

    return False
