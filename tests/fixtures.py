NEAREST_STATION_RESPONSE = [
    {
        "geohash": "gfxnj5",
        "name": "LERWICK",
        "distance": 0.5
    }
]

OBSERVATIONS_RESPONSE = [
    {
        "datetime": "2026-02-11T12:00:00Z",
        "humidity": "85",
        "mslp": "1013",
        "pressure_tendency": "F",
        "temperature": "8.5",
        "visibility": "15000",
        "weather_code": "7",
        "wind_direction": "SW",
        "wind_gust": "25",
        "wind_speed": "18"
    },
    {
        "datetime": "2026-02-11T13:00:00Z",
        "humidity": "82",
        "mslp": "1014",
        "pressure_tendency": "R",
        "temperature": "9.2",
        "visibility": "20000",
        "weather_code": "3",
        "wind_direction": "W",
        "wind_gust": "22",
        "wind_speed": "16"
    }
]

SAMPLE_SITE = {
    "site_id": "3005",
    "site_name": "LERWICK (S. SCREEN)",
    "site_country": "SCOTLAND",
    "site_elevation": 82.0,
    "lat": 60.139,
    "lon": -1.183
}

EXPECTED_TRANSFORMED_ROW = {
    "observation_ts": "2026-02-11 12:00:00",
    "site_id": "3005",
    "site_name": "LERWICK (S. SCREEN)",
    "site_country": "SCOTLAND",
    "site_continent": "EUROPE",
    "site_elevation": 82.0,
    "lat": 60.139,
    "lon": -1.183,
    "wind_direction": "SW",
    "wind_gust": "25",
    "screen_relative_humidity": "85",
    "pressure": "1013",
    "wind_speed": "18",
    "temperature": "8.5",
    "visibility": "15000",
    "weather_type": "7",
    "pressure_tendency": "F",
}
