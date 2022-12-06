import boto3
from time import sleep


# Should be using https://github.com/laughingman7743/PyAthena

def load_file_to_s3(filename, s3_bucket, s3_key):
    print("Uploading data to S3://{0}/{1}".format(s3_bucket, s3_key))

    s3 = boto3.resource('s3')
    s3.Bucket(s3_bucket).upload_file(
        filename,
        s3_key
    )


def download_file_from_s3(s3_bucket, s3_key, filename):
    print("Downloading data from S3://{0}/{1}".format(s3_bucket, s3_key))

    s3 = boto3.resource('s3')
    s3.Bucket(s3_bucket).download_file(
        s3_key,
        filename
    )


def delete_folder_from_s3(s3_bucket, folder_key):
    print("Deleting all files in S3://{0}/{1}".format(s3_bucket, folder_key))

    s3 = boto3.resource('s3')

    objects = s3.Bucket(s3_bucket).objects.filter(Prefix=folder_key)

    for f in objects:
        print("Deleting: " + f.key)
        f.delete()


def add_glue_partition_for(year, month, day):
    sql = f"ALTER TABLE weather ADD IF NOT EXISTS PARTITION (year='{year}', month='{month}', day='{day}')"
    execute_athena_command(sql)


def execute_athena_command(sql: str, wait_seconds: int = 10):
    athena = boto3.client('athena')

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

    for count in range(wait_seconds):
        query_execution = athena.get_query_execution(QueryExecutionId=query_start['QueryExecutionId'])
        state = query_execution.get('QueryExecution', {}).get('Status', {}).get('State')

        if state == 'FAILED':
            print("Query failed : " + query_execution.get('QueryExecution', {}).get('Status', {}).get(
                'StateChangeReason'))
            break
        elif state == 'SUCCEEDED':
            print('Query succeeded')
            break

        sleep(1)

    return False


def execute_athena_query(sql: str, wait_seconds: int = 30):
    query_results_bucket = "dantelore.queryresults"  # Not the best place to store this - but it's only for notebooks...

    athena = boto3.client('athena')

    query_start = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={
            'Database': "incoming"
        },
        ResultConfiguration={
            'OutputLocation': f"s3://{query_results_bucket}/Unsaved/"
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
