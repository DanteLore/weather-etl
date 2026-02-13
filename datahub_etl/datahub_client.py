import requests
import time


class DataHubClient:
    def __init__(self, api_key, base_url="https://data.hub.api.metoffice.gov.uk/observation-land/1"):
        self.api_key = api_key
        self.base_url = base_url

    def get_headers(self):
        return {"apikey": self.api_key}

    def _request_with_backoff(self, url, params=None, max_retries=3):
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.get_headers(), params=params)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) * 1
                        print(f"Rate limit hit, waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
                        time.sleep(wait_time)
                        continue
                raise

    def get_nearest_station(self, lat, lon):
        url = f"{self.base_url}/nearest"
        params = {"lat": round(lat, 2), "lon": round(lon, 2)}
        return self._request_with_backoff(url, params=params)

    def get_observations(self, geohash):
        url = f"{self.base_url}/{geohash}"
        return self._request_with_backoff(url)
