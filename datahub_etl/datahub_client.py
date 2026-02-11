import requests


class DataHubClient:
    def __init__(self, api_key, base_url="https://data.hub.api.metoffice.gov.uk/observation-land/1"):
        self.api_key = api_key
        self.base_url = base_url

    def get_headers(self):
        return {"apikey": self.api_key}

    def get_nearest_station(self, lat, lon):
        url = f"{self.base_url}/nearest"
        params = {"lat": round(lat, 2), "lon": round(lon, 2)}
        response = requests.get(url, headers=self.get_headers(), params=params)
        response.raise_for_status()
        return response.json()

    def get_observations(self, geohash):
        url = f"{self.base_url}/{geohash}"
        response = requests.get(url, headers=self.get_headers())
        response.raise_for_status()
        return response.json()
