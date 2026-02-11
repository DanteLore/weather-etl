from datahub_etl.weather_etl import extract_observations_data, transform_observations_data
from datahub_etl.datahub_client import DataHubClient
from datahub_etl.api_key import API_KEY

INPUT_FILE = 'weatherData/datahub_raw.json'
OUTPUT_FILE = 'weatherData/datahub_observations.json'

if __name__ == '__main__':
    print("Fetching observations from Met Office DataHub API...")
    client = DataHubClient(API_KEY)
    extract_observations_data(INPUT_FILE, client)

    print("\nTransforming observations to legacy format...")
    transform_observations_data(INPUT_FILE, OUTPUT_FILE)

    print("\nDone! Check the output files:")
    print(f"  Raw: {INPUT_FILE}")
    print(f"  Transformed: {OUTPUT_FILE}")