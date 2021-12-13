import requests
import json
from datetime import datetime, timedelta
from api_key import API_KEY
import boto3

BASE_URL = "http://datapoint.metoffice.gov.uk/public/data/"
BASE_PARAMS = {
    "key": API_KEY,
    "res": "hourly"
}

TEMP_FILE = "/tmp/observations.json"
S3_BUCKET = "dantelore.data.incoming"
S3_FILENAME = "weather/{year}/{month}/{day}/observations.json".format(year=datetime.now().year,
                                                                      month=datetime.now().month,
                                                                      day=datetime.now().day)


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


def extract_observations_data():
    url = BASE_URL + "val/wxobs/all/json/all"
    response = requests.get(url, params=BASE_PARAMS)

    print("Received status {0} from {1}".format(response.status_code, url))

    if response.status_code == 200:
        return response.json()
    else:
        return None


def transform_observations_data(data):
    params = data['SiteRep']['Wx']['Param']
    names_lookup = dict([(p['name'], p['$'].lower().replace(' ', '_')) for p in params])
    obs = data['SiteRep']['DV']['Location']

    print("Transforming weatherData for {0} sites".format(len(obs)))

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

            for r in p['Rep']:
                try:
                    time = day + timedelta(minutes=int(r['$']))
                    row = {
                        "timestamp": time.isoformat(),
                        "site_id": site_id,
                        "site_name": site_name,
                        "site_country": site_country,
                        "site_continent": site_continent,
                        "site_elevation": site_elevation,
                        "lat": lat,
                        "lon": lon
                    }

                    observations = dict((names_lookup[key], r[key]) for key in r if key != '$')
                    row.update(observations)
                    yield json.dumps(row)
                except Exception as e:
                    print("Parsing data row in 'Rep' failed")
                    print(e)


def load_observations_data_to_local(filename):
    data = extract_observations_data()

    if not data:
        return False

    with open(filename, 'w') as f:
        i = 0
        for line in transform_observations_data(data):
            i += 1
            f.writelines(line + '\n')

        print("Wrote {0} lines to file '{1}'".format(i, filename))

    return True


def load_observations_data_to_s3():
    if load_observations_data_to_local(TEMP_FILE):
        s3 = boto3.resource('s3')
        s3.Bucket(S3_BUCKET).upload_file(
            TEMP_FILE,
            S3_FILENAME
        )
