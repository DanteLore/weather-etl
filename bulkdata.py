from concurrent.futures import ThreadPoolExecutor

import requests
import os
import csv
import json
import gzip
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from datetime import datetime

from aws_helpers import load_file_to_s3
from ceda_auth_helpers import setup_credentials, CREDENTIALS_FILE_PATH, CERTS_DIR

LOCAL_FILE_STORE = "weatherData/midas"
ROOT_URL = "https://dap.ceda.ac.uk/badc/ukmo-midas-open/data/uk-hourly-weather-obs/dataset-version-202107/"
S3_RAW_BUCKET = "dantelore.data.raw"
S3_INCOMING_BUCKET = "dantelore.data.incoming"
S3_BASE_KEY = "midas"
VERSION_ID = "qc-version-1"

history = []
executor = ThreadPoolExecutor(max_workers=8)


def save_file(response, url):
    dirs = url[len(ROOT_URL):].split('/')[:-1]
    dest_dir = os.path.join(LOCAL_FILE_STORE, *dirs)

    filename = url.rsplit('/', 1)[-1]
    local_file = os.path.join(dest_dir, filename)

    ensure_directory_exists(dest_dir)

    print(f"Saving file: {local_file}")
    with open(local_file, 'wb') as file_object:
        file_object.write(response.content)

    return local_file


def save_gzipped_file_to_s3(bucket, local_file):
    dirs = local_file[len(LOCAL_FILE_STORE):].split('/')[:-1]

    zipped_file = local_file + ".gz"
    with open(local_file, 'rb') as f_in, gzip.open(zipped_file, 'wb') as f_out:
        f_out.writelines(f_in)

    raw_s3_key = os.path.join(S3_BASE_KEY, *dirs, zipped_file.rsplit('/', 1)[-1])
    load_file_to_s3(zipped_file, bucket, raw_s3_key)


def fetch_data(url):
    # Don't visit anywhere twice, don't go higher than the root URL
    if url in history or (url != ROOT_URL and ROOT_URL.startswith(url)):
        return

    history.append(url)
    response = requests.get(url, cert=CREDENTIALS_FILE_PATH, verify=CERTS_DIR)

    content_type = response.headers['content-type']
    if content_type == 'application/octet-stream':
        if VERSION_ID in url: # Note that we only want one of the QA versions here, to avoid duplicates.
            process_data_file(response, url)
    elif content_type == 'text/html':
        soup = BeautifulSoup(response.content, features="html.parser")

        for link in soup.find_all('a', href=True):
            link = link['href']
            child_url = urljoin(url, link)
            fetch_data(child_url)
    else:
        print('Unknown content type')


def extract_data(csv_filename):
    print('Processing: ' + csv_filename)

    csv_lines = []
    data = {}
    in_csv_section = False
    year = None

    with open(csv_filename, "r") as f:
        for line in f:
            if line.lower().strip() == 'data':
                in_csv_section = True
            elif line.lower().strip() == "end data":
                in_csv_section = False
            elif line.startswith('date_valid'):
                year = datetime.strptime(line.split(',')[-2].strip(), '%Y-%m-%d %H:%M:%S').year
            elif in_csv_section:
                csv_lines.append(line)
            elif line.startswith('observation_station,'):
                data["observation_station"] = line.split(',')[-1].strip()
            elif line.startswith('location,'):
                data["lon"] = float(line.split(',')[-1])
                data["lat"] = float(line.split(',')[-2])
            elif line.startswith('height,'):
                data["height"] = line.split(',')[-2].strip()
            elif line.startswith('historic_county_name,'):
                data["county"] = line.split(',')[-1].strip()

    if len(csv_lines) == 0:
        print("No data in file")
        return

    if not year:
        print("Cannot determine year")
        return

    json_dir = os.path.join(LOCAL_FILE_STORE, f"year={year}")

    ensure_directory_exists(json_dir)

    json_filename = os.path.join(json_dir, csv_filename.split('/')[-1].replace('.csv', '.json'))

    print("Writing JSON to " + json_filename)
    with open(json_filename, "w") as f:
        for row in csv.DictReader(csv_lines):
            # Combine file and row data, remove anything which is 'NA' to save space
            output_row = {key: val for key, val in {**data, **row}.items() if val != 'NA'}
            json_line = json.dumps(output_row)
            f.write(json_line + "\n")
    return json_filename


def ensure_directory_exists(d):
    if not os.path.exists(d):
        os.makedirs(d)


def do_process_data_file(response, url):
    raw_data_file = save_file(response, url)
    if raw_data_file:
        save_gzipped_file_to_s3(S3_RAW_BUCKET, raw_data_file)
        json_file = extract_data(raw_data_file)
        if json_file:
            save_gzipped_file_to_s3(S3_INCOMING_BUCKET, json_file)

            # Zipped copies will remain
            os.remove(json_file)
        os.remove(raw_data_file)


def process_data_file(response, url):
    executor.submit(do_process_data_file, response, url)


if __name__ == '__main__':
    try:
        setup_credentials()
        print("Credentials setup complete")
    except KeyError:
        print("CEDA_USERNAME and CEDA_PASSWORD environment variables required")
        exit(1)

    fetch_data(ROOT_URL)

