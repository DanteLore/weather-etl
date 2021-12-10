from weather_etl import load_observations_data


def handler(event, context):
    load_observations_data()
