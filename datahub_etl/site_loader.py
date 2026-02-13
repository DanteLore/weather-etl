import csv
import io
from typing import List, Dict

_SITES_CACHE = None


def _parse_s3_location(s3_url: str) -> tuple[str, str]:
    parts = s3_url.split('/')
    bucket = parts[2]
    key = '/'.join(parts[3:])
    return bucket, key


def _parse_sites_csv(csv_content: str) -> List[Dict]:
    sites = []
    reader = csv.DictReader(io.StringIO(csv_content))
    for row in reader:
        sites.append({
            'site_id': row['site_id'],
            'site_name': row['site_name'],
            'site_country': row['site_country'],
            'site_elevation': float(row['site_elevation']),
            'lat': float(row['lat']),
            'lon': float(row['lon'])
        })
    return sites


def load_sites_from_athena(
    database: str = "lake",
    results_bucket: str = "dantelore.queryresults",
    wait_seconds: int = 30
) -> List[Dict]:
    global _SITES_CACHE

    if _SITES_CACHE is not None:
        print(f"Using cached site data ({len(_SITES_CACHE)} sites)")
        return _SITES_CACHE

    print("Loading site data from Athena...")

    from helpers.aws import execute_athena_query, load_text_from_s3

    sql = """
    SELECT site_id, site_name, site_country, site_elevation, lat, lon
    FROM lake.weather_stations
    ORDER BY site_id
    """

    output_location = execute_athena_query(sql, database, results_bucket, wait_seconds)
    bucket, key = _parse_s3_location(output_location)
    csv_content = load_text_from_s3(bucket, key)
    sites = _parse_sites_csv(csv_content)

    _SITES_CACHE = sites
    print(f"Loaded {len(sites)} sites from Athena")
    return sites


def get_sites(
    database: str = "lake",
    results_bucket: str = "dantelore.queryresults"
) -> List[Dict]:
    return load_sites_from_athena(database, results_bucket)


def clear_cache():
    global _SITES_CACHE
    _SITES_CACHE = None
