from helpers.aws import execute_athena_command
from datetime import datetime

S3_INCOMING_BUCKET = "dantelore.data.incoming"
S3_DATA_LAKE_BUCKET = "dantelore.data.lake"


def build_data_models(incoming_bucket, data_lake_bucket):
    print("Modelled!")


def handler(event, context):
    try:
        build_data_models(S3_INCOMING_BUCKET, S3_DATA_LAKE_BUCKET)
    except Exception as e:
        print("Failed to transform data")
        print(e)
        return {"statusCode": 500}
