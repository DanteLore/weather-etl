import json
import os
from datetime import datetime, timezone
from site_loader import get_sites

CACHE_FILE = "/tmp/geohash_cache.json"


def load_geohash_cache(cache_file=CACHE_FILE, s3_bucket=None, s3_key=None):
    if s3_bucket and s3_key:
        try:
            from helpers.aws import load_json_from_s3
            cache = load_json_from_s3(s3_bucket, s3_key)
            if cache:
                print(f"Loaded geohash cache from S3://{s3_bucket}/{s3_key}")
                return cache
        except Exception as e:
            print(f"Could not load cache from S3: {e}")

    if os.path.exists(cache_file):
        with open(cache_file, 'r') as f:
            cache = json.load(f)
            print(f"Loaded geohash cache from {cache_file} ({len(cache)} sites)")
            return cache

    print("No geohash cache found, will populate from API")
    return {}


def save_geohash_cache(cache, cache_file=CACHE_FILE, s3_bucket=None, s3_key=None):
    with open(cache_file, 'w') as f:
        json.dump(cache, f, indent=2)
        print(f"Saved geohash cache to {cache_file} ({len(cache)} sites)")

    if s3_bucket and s3_key:
        try:
            from helpers.aws import save_json_to_s3
            save_json_to_s3(cache, s3_bucket, s3_key)
        except Exception as e:
            print(f"Could not save cache to S3: {e}")


def extract_observations_data(filename, client, s3_bucket=None, s3_cache_key=None):
    geohash_cache = load_geohash_cache(s3_bucket=s3_bucket, s3_key=s3_cache_key)
    cache_updated = False
    all_observations = []
    failed_sites = []

    all_sites = get_sites()
    batch_size = max(1, len(all_sites) // 24)

    sites_with_priority = []
    for site in all_sites:
        site_id = site["site_id"]
        cache_entry = geohash_cache.get(site_id, {})

        if isinstance(cache_entry, str):
            cache_entry = {"geohash": cache_entry, "last_fetched": None}
            geohash_cache[site_id] = cache_entry
            cache_updated = True

        last_fetched = cache_entry.get("last_fetched")
        sites_with_priority.append((site, last_fetched or "1970-01-01T00:00:00Z"))

    sites_with_priority.sort(key=lambda x: x[1])
    sites_to_fetch = [site for site, _ in sites_with_priority[:batch_size]]

    print(f"Processing batch of {len(sites_to_fetch)} sites (out of {len(all_sites)} total, batch_size={batch_size})")

    for site in sites_to_fetch:
        site_id = site["site_id"]

        try:
            cache_entry = geohash_cache.get(site_id, {})
            geohash = cache_entry.get("geohash") if isinstance(cache_entry, dict) else cache_entry

            if not geohash:
                station = client.get_nearest_station(site["lat"], site["lon"])
                if not station:
                    print(f"No station found for {site['site_name']}")
                    failed_sites.append(site["site_name"])
                    continue

                geohash = station[0]["geohash"]
                print(f"  Cached geohash for {site['site_name']}: {geohash}")

            observations = client.get_observations(geohash)

            if not observations:
                print(f"No observations for {site['site_name']}")
                failed_sites.append(site["site_name"])
                continue

            for obs in observations:
                obs["_site_metadata"] = site
                obs["_geohash"] = geohash
                all_observations.append(obs)

            geohash_cache[site_id] = {
                "geohash": geohash,
                "last_fetched": datetime.now(timezone.utc).isoformat()
            }
            cache_updated = True

            print(f"  {site['site_name']}: {len(observations)} observations")

        except Exception as e:
            print(f"Failed to fetch {site['site_name']}: {e}")
            failed_sites.append(site["site_name"])

    if cache_updated:
        save_geohash_cache(geohash_cache, s3_bucket=s3_bucket, s3_key=s3_cache_key)

    if failed_sites:
        print(f"\nFailed to fetch {len(failed_sites)} sites")

    with open(filename, 'w') as f:
        json.dump(all_observations, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(all_observations)} observations to {filename}")
    return len(all_observations) > 0


def transform_observations_data(input_filename, output_filename):
    with open(input_filename, 'r') as f:
        observations = json.load(f)

    if not observations:
        return False

    with open(output_filename, 'w') as f:
        count = 0
        for obs in observations:
            row = transform_observation(obs)
            if row:
                f.write(json.dumps(row) + '\n')
                count += 1

        print(f"Wrote {count} lines to {output_filename}")

    return True


def transform_observation(obs):
    site = obs.get("_site_metadata")
    if not site:
        return None

    return {
        "observation_ts": obs.get("datetime", "").replace("Z", "").replace("T", " "),
        "site_id": site["site_id"],
        "site_name": site["site_name"],
        "site_country": site["site_country"],
        "site_continent": "EUROPE",
        "site_elevation": float(site["site_elevation"]),
        "lat": float(site["lat"]),
        "lon": float(site["lon"]),
        "wind_direction": obs.get("wind_direction", ""),
        "wind_gust": obs.get("wind_gust", ""),
        "screen_relative_humidity": obs.get("humidity", ""),
        "pressure": obs.get("mslp", ""),
        "wind_speed": obs.get("wind_speed", ""),
        "temperature": obs.get("temperature", ""),
        "visibility": obs.get("visibility", ""),
        "weather_type": obs.get("weather_code", ""),
        "pressure_tendency": obs.get("pressure_tendency", ""),
    }
