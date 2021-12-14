import json
from datetime import datetime, timedelta
import requests
from api_key import API_KEY

BASE_URL = "http://datapoint.metoffice.gov.uk/public/data/"
BASE_PARAMS = {
    "key": API_KEY,
    "res": "hourly"
}


def get_sites():
    url = BASE_URL + "val/wxobs/all/json/sitelist"
    response = requests.get(url, params=BASE_PARAMS)
    if response.status_code == 200:
        data = response.json()
        locations = data['Locations']['Location']
        print(len(locations))
        for loc in locations:
            print(loc)


def print_observations_fields():
    url = BASE_URL + "val/wxobs/all/json/all"
    response = requests.get(url, params=BASE_PARAMS)
    if response.status_code == 200:
        data = response.json()
        params = data['SiteRep']['Wx']['Param']

        for p in params:
            print("{name}: {$} ({units})".format(**p))


def extract_observations_data(filename):
    url = BASE_URL + "val/wxobs/all/json/all"
    response = requests.get(url, params=BASE_PARAMS)

    print("Received status {0} from {1}".format(response.status_code, url))

    if response.status_code == 200:
        with open(filename, 'w') as f:
            json.dump(response.json(), f, ensure_ascii=False, indent=4)

        return True
    else:
        return False


def do_transform(data):
    params = data['SiteRep']['Wx']['Param']
    names_lookup = dict([(p['name'], p['$'].lower().replace(' ', '_')) for p in params])
    obs = data['SiteRep']['DV']['Location']

    print("Transforming Weather Data for {0} sites".format(len(obs)))

    for o in obs:
        site_id = o['i']
        site_name = o['name']
        site_country = o['country']
        site_continent = o['continent']
        site_elevation = o['elevation']
        lat = o['lat']
        lon = o['lon']

        # Periods might be a single record or an array :/
        periods = o['Period']
        if not isinstance(periods, list):
            periods = [periods]

        for p in periods:
            day = datetime.strptime(p['value'], "%Y-%m-%dZ")

            # 'Rep' could also be a list or a single item
            obs = p['Rep']
            if not isinstance(obs, list):
                obs = [obs]

            for r in obs:
                try:
                    time = day + timedelta(minutes=int(r['$']))
                    row = {
                        "observation_ts": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "site_id": site_id,
                        "site_name": site_name,
                        "site_country": site_country,
                        "site_continent": site_continent,
                        "site_elevation": site_elevation,
                        "lat": lat,
                        "lon": lon
                    }
                except:
                    print("Borkenz")
                    return

                observations = dict((names_lookup[key], r[key]) for key in r if key != '$')
                row.update(observations)
                yield json.dumps(row)


def transform_observations_data(input_filename, output_filename):
    with open(input_filename, 'r') as f:
        data = json.load(f)

    if not data:
        return False

    with open(output_filename, 'w') as f:
        i = 0
        for line in do_transform(data):
            i += 1
            f.writelines(line + '\n')

        print("Wrote {0} lines to file '{1}'".format(i, output_filename))

    return True

