from aws_helpers import add_glue_partition_for
from datetime import datetime
from weather_etl import extract_observations_data, transform_observations_data

# https://www.metoffice.gov.uk/services/data/datapoint

INPUT_FILE = 'response.json'
OUTPUT_FILE = 'weatherData/last_24h_observations_uk.json'

if __name__ == '__main__':
    #extract_observations_data(INPUT_FILE)
    #transform_observations_data(INPUT_FILE, OUTPUT_FILE)

    today = datetime.today()
    add_glue_partition_for(today.year, today.month, today.day)