from datapoint_etl.weather_etl import extract_observations_data, transform_observations_data, validate_json

# https://www.metoffice.gov.uk/services/data/datapoint

INPUT_FILE = 'weatherData/response.json'
OUTPUT_FILE = 'weatherData/last_24h_observations_uk.json'
JSON_SCHEMA_FILE = 'datapoint_etl/weather_schema.json'

if __name__ == '__main__':
    extract_observations_data(INPUT_FILE)
    transform_observations_data(INPUT_FILE, OUTPUT_FILE)

    print("Validating data against JSON schema...")
    validate_json(OUTPUT_FILE, JSON_SCHEMA_FILE)
    print("Done!")

    #today = datetime.today()
    #add_glue_partition_for(today.year, today.month, today.day)