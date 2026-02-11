import boto3
import json
from time import sleep
from botocore.exceptions import ClientError


# Should be using https://github.com/laughingman7743/PyAthena

def load_file_to_s3(filename, s3_bucket, s3_key):
    print(f"Uploading data to S3://{s3_bucket}/{s3_key}")

    s3 = boto3.client('s3')
    s3.upload_file(filename, s3_bucket, s3_key)


def download_file_from_s3(s3_bucket, s3_key, filename):
    print(f"Downloading data from S3://{s3_bucket}/{s3_key}")

    s3 = boto3.client('s3')
    s3.download_file(s3_bucket, s3_key, filename)


def delete_folder_from_s3(s3_bucket, folder_key):
    print(f"Deleting all files in S3://{s3_bucket}/{folder_key}")

    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=s3_bucket, Prefix=folder_key):
        if 'Contents' in page:
            for obj in page['Contents']:
                print(f"Deleting: {obj['Key']}")
                s3.delete_object(Bucket=s3_bucket, Key=obj['Key'])


def load_text_from_s3(s3_bucket, s3_key):
    """Load text content from S3 (CSV, plain text, etc.)"""
    try:
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
        content = response['Body'].read().decode('utf-8')
        return content
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return None
        print(f"Failed to load text from S3://{s3_bucket}/{s3_key}: {e}")
        return None
    except Exception as e:
        print(f"Failed to load text from S3://{s3_bucket}/{s3_key}: {e}")
        return None


def load_json_from_s3(s3_bucket, s3_key):
    try:
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return None
        print(f"Failed to load JSON from S3://{s3_bucket}/{s3_key}: {e}")
        return None
    except Exception as e:
        print(f"Failed to load JSON from S3://{s3_bucket}/{s3_key}: {e}")
        return None


def save_json_to_s3(data, s3_bucket, s3_key):
    try:
        s3 = boto3.client('s3')
        s3.put_object(
            Bucket=s3_bucket,
            Key=s3_key,
            Body=json.dumps(data, indent=2),
            ContentType='application/json'
        )
        print(f"Saved JSON to S3://{s3_bucket}/{s3_key}")
        return True
    except Exception as e:
        print(f"Failed to save JSON to S3://{s3_bucket}/{s3_key}: {e}")
        return False


def add_glue_partition_for(year, month, day, table, database, results_bucket):
    sql = f"ALTER TABLE {table} ADD IF NOT EXISTS PARTITION (year='{year}', month='{month}', day='{day}')"
    execute_athena_command(sql, database, results_bucket)


def execute_athena_command(sql, database, results_bucket, wait_seconds=10):
    athena = boto3.client('athena')

    print(f"Executing: {sql}")

    query_start = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': f"s3://{results_bucket}/Unsaved/"
        }
    )

    for count in range(wait_seconds):
        query_execution = athena.get_query_execution(QueryExecutionId=query_start['QueryExecutionId'])
        state = query_execution.get('QueryExecution', {}).get('Status', {}).get('State')

        if state == 'FAILED':
            reason = query_execution.get('QueryExecution', {}).get('Status', {}).get('StateChangeReason')
            print(f"Query failed: {reason}")
            return False
        elif state == 'SUCCEEDED':
            print('Query succeeded')
            return True

        sleep(1)

    print(f"Query timed out after {wait_seconds} seconds")
    return False


def execute_athena_query(sql, database, results_bucket, wait_seconds=30):
    athena = boto3.client('athena')

    query_start = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': f"s3://{results_bucket}/Unsaved/"
        }
    )

    for count in range(wait_seconds):
        query_execution = athena.get_query_execution(QueryExecutionId=query_start['QueryExecutionId'])
        state = query_execution.get('QueryExecution', {}).get('Status', {}).get('State')

        if state == 'FAILED':
            print(
                "Query failed: " + query_execution.get('QueryExecution', {}).get('Status', {}).get('StateChangeReason')
            )
            return None
        elif state == 'SUCCEEDED':
            return query_execution['QueryExecution']['ResultConfiguration']['OutputLocation']

        sleep(1)

    print(f"Query timed out after {wait_seconds} attempts")
    return None
