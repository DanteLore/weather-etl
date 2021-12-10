from weather_etl import load_observations_data_to_s3


def handler(event, context):
    load_observations_data_to_s3()
