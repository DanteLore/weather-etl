import requests
import os
from urllib.parse import urljoin
from bs4 import BeautifulSoup

from aws_helpers import load_file_to_s3
from ceda_auth_helpers import setup_credentials, CREDENTIALS_FILE_PATH, CERTS_DIR

DEST_DIR = "weatherData/midas"
ROOT_URL = "https://dap.ceda.ac.uk/badc/ukmo-midas-open/data/uk-hourly-weather-obs/dataset-version-202107/"
S3_RAW_BUCKET = "dantelore.data.raw"
S3_BASE_KEY = "midas"

history = []


def save_file(response, url):
    dirs = url[len(ROOT_URL):].split('/')[:-1]
    dest_dir = os.path.join(DEST_DIR, *dirs)

    filename = url.rsplit('/', 1)[-1]
    local_file = os.path.join(dest_dir, filename)

    raw_s3_key = os.path.join(S3_BASE_KEY, *dirs, filename)

    if not os.path.exists(dest_dir):
        print('Created directory: ' + dest_dir)
        os.makedirs(dest_dir)

    print(f"Saving file: {local_file}")
    with open(local_file, 'wb') as file_object:
        file_object.write(response.content)

    load_file_to_s3(local_file, S3_RAW_BUCKET, raw_s3_key)


def fetch_data(url):
    # Don't visit anywhere twice, don't go higher than the root URL
    if url in history or (url != ROOT_URL and ROOT_URL.startswith(url)):
        return

    print("Processing " + url)

    history.append(url)
    response = requests.get(url, cert=CREDENTIALS_FILE_PATH, verify=CERTS_DIR)

    content_type = response.headers['content-type']
    if content_type == 'application/octet-stream':
        save_file(response, url)
    elif content_type == 'text/html':
        soup = BeautifulSoup(response.content)

        for link in soup.find_all('a', href=True):
            link = link['href']
            child_url = urljoin(url, link)
            fetch_data(child_url)
    else:
        print('Unknown content type')


if __name__ == '__main__':
    try:
        setup_credentials()
    except KeyError:
        print("CEDA_USERNAME and CEDA_PASSWORD environment variables required")
        exit(1)

    fetch_data(ROOT_URL)

