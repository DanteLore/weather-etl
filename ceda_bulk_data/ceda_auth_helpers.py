import os
import datetime
import requests
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from contrail.security.onlineca.client import OnlineCaClient

# Grabbed from here: https://github.com/cedadev/opendap-python-example/blob/master/simple_file_downloader.py

CERTS_DIR = os.path.expanduser('~/.certs')
if not os.path.isdir(CERTS_DIR):
    os.makedirs(CERTS_DIR)

TRUSTROOTS_DIR = os.path.join(CERTS_DIR, 'ca-trustroots')
CREDENTIALS_FILE_PATH = os.path.join(CERTS_DIR, 'credentials.pem')

TRUSTROOTS_SERVICE = 'https://slcs.ceda.ac.uk/onlineca/trustroots/'
CERT_SERVICE = 'https://slcs.ceda.ac.uk/onlineca/certificate/'


def cert_is_valid(cert_file, min_lifetime=0):
    try:
        with open(cert_file, 'rb') as f:
            crt_data = f.read()
    except IOError:
        return False

    try:
        cert = x509.load_pem_x509_certificate(crt_data, default_backend())
    except ValueError:
        return False

    now = datetime.datetime.now()

    return (cert.not_valid_before <= now
            and cert.not_valid_after > now + datetime.timedelta(0, min_lifetime))


def setup_credentials():
    # Test for DODS_FILE and only re-get credentials if it doesn't
    # exist AND `force` is True AND certificate is in-date.
    if cert_is_valid(CREDENTIALS_FILE_PATH):
        print('[INFO] Security credentials already set up.')
        return False

    # Get CEDA username and password from environment variables
    username = os.environ['CEDA_USERNAME']
    password = os.environ['CEDA_PASSWORD']

    onlineca_client = OnlineCaClient()
    onlineca_client.ca_cert_dir = TRUSTROOTS_DIR

    # Set up trust roots
    trustroots = onlineca_client.get_trustroots(
        TRUSTROOTS_SERVICE,
        bootstrap=True,
        write_to_ca_cert_dir=True)

    # Write certificate credentials file
    key_pair, certs = onlineca_client.get_certificate(
        username,
        password,
        CERT_SERVICE,
        pem_out_filepath=CREDENTIALS_FILE_PATH)

    print('[INFO] Security credentials set up.')
    return True