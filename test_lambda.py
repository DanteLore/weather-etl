import sys
import json
import boto3
from datetime import datetime
import time


def invoke_lambda(function_name):
    lambda_client = boto3.client('lambda')

    print(f"Invoking Lambda function: {function_name}")
    print(f"Time: {datetime.now()}\n")

    try:
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            LogType='Tail'
        )

        status_code = response['StatusCode']
        print(f"Status Code: {status_code}")

        if 'FunctionError' in response:
            print(f"\n[ERROR] Function Error: {response['FunctionError']}")

        payload = json.loads(response['Payload'].read())
        print(f"\nResponse Payload:")
        print(json.dumps(payload, indent=2))

        if 'LogResult' in response:
            import base64
            logs = base64.b64decode(response['LogResult']).decode('utf-8')
            print(f"\n{'='*60}")
            print("Lambda Execution Logs:")
            print('='*60)
            print(logs)

        return status_code == 200 and 'FunctionError' not in response

    except Exception as e:
        print(f"\n[ERROR] Error invoking Lambda: {e}")
        return False


def get_recent_logs(function_name, minutes=5):
    logs_client = boto3.client('logs')
    log_group = f"/aws/lambda/{function_name}"

    print(f"\nFetching recent logs from: {log_group}")

    try:
        end_time = int(time.time() * 1000)
        start_time = end_time - (minutes * 60 * 1000)

        response = logs_client.filter_log_events(
            logGroupName=log_group,
            startTime=start_time,
            endTime=end_time,
            limit=100
        )

        if response['events']:
            print(f"\nRecent log events (last {minutes} minutes):")
            print('='*60)
            for event in response['events']:
                timestamp = datetime.fromtimestamp(event['timestamp'] / 1000)
                print(f"[{timestamp}] {event['message']}")
        else:
            print(f"No log events found in the last {minutes} minutes")

    except Exception as e:
        print(f"Error fetching logs: {e}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python test_lambda.py <function_name>")
        print("\nAvailable functions:")
        print("  - load_weather_data")
        print("  - model_weather_data")
        sys.exit(1)

    function_name = sys.argv[1]
    success = invoke_lambda(function_name)

    if not success:
        print("\n" + "="*60)
        get_recent_logs(function_name, minutes=10)

    print("\n" + "="*60)
    print(f"Test {'[SUCCESS] PASSED' if success else '[ERROR] FAILED'}")
    sys.exit(0 if success else 1)