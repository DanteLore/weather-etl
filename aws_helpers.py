import boto3
from time import sleep


def load_file_to_s3(filename, s3_bucket, s3_key):
    print("Uploading data to S3://{0}/{1}".format(s3_bucket, s3_key))

    s3 = boto3.resource('s3')
    s3.Bucket(s3_bucket).upload_file(
        filename,
        s3_key
    )


def add_glue_partition_for(year, month, day):
    sql = f"ALTER TABLE weather ADD IF NOT EXISTS PARTITION (year='{year}', month='{month}', day='{day}')"
    execute_athena_command(sql)


def execute_athena_command(sql):
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

    for count in range(10):
        query_execution = athena.get_query_execution(QueryExecutionId=query_start['QueryExecutionId'])
        state = query_execution.get('QueryExecution', {}).get('Status', {}).get('State')

        if state == 'FAILED':
            print("Query failed : " + query_execution.get('QueryExecution', {}).get('Status', {}).get(
                'StateChangeReason'))
            break
        elif state == 'SUCCEEDED':
            print('Query succeeded')
            break

        print(f"Wait count {count}/10")
        sleep(1)

    return False

