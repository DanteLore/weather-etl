from weather_etl import load_observations_data_to_local

# https://www.metoffice.gov.uk/services/data/datapoint


if __name__ == '__main__':
    load_observations_data_to_local('weatherData/last_24h_observations_uk.json')
