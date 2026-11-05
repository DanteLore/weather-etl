#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"

# Bootstrap: check tools and AWS login, create the Python venv and initialise Terraform,
# so a fresh clone can build and deploy with nothing set up by hand.
export AWS_PROFILE="${AWS_PROFILE:-dantelore}"

for tool in python3 terraform aws; do
    command -v "$tool" > /dev/null || { echo "Missing $tool (Terraform: brew install hashicorp/tap/terraform)"; exit 1; }
done
aws sts get-caller-identity > /dev/null || { echo "AWS login expired: aws sso login --profile $AWS_PROFILE"; exit 1; }

# API keys are git-ignored and get bundled into the Lambda zips, so never build without them
[ -f datahub_etl/api_key.py ] || { echo "Missing datahub_etl/api_key.py: Met Office DataHub key (template: datahub_etl/api_key.example.py)"; exit 1; }
if cmp -s datahub_etl/api_key.py datahub_etl/api_key.example.py; then echo "datahub_etl/api_key.py is still the template: add the real Met Office DataHub key"; exit 1; fi

if [ ! -d .venv ]; then
    echo "Creating Python venv..."
    python3 -m venv .venv
fi
source .venv/bin/activate

# Install requirements on first run, and again whenever a requirements file changes
if [ ! -f .venv/.installed ] || [ -n "$(find requirements*.txt -newer .venv/.installed)" ]; then
    echo "Installing Python requirements..."
    pip install --quiet -r requirements.txt
    touch .venv/.installed
fi

echo "Initialising Terraform..."
terraform -chdir=terraform init -input=false -reconfigure > /dev/null

echo "Running tests..."
pytest tests/ || exit 1

echo "Building the Weather ETL"
mkdir build || exit

cp -Rf datahub_etl build
cp -Rf helpers build

(
  cd build || exit
  pip install --quiet --target . -r ../requirements-lambda.txt
  zip -qq -r ../terraform/weather_etl.zip ./*
)
rm -rf build

echo "Uploading weather station metadata to S3"
aws s3 cp datahub_etl/sites.json s3://dantelore.data.lake/weather_stations/sites.json

echo "Building the Weather Data Modeller"
mkdir build

cp weather_data_model/lambda_function.py build
cp -Rf helpers build

(
  cd build || exit
  pip install --quiet --target . -r ../requirements-lambda.txt
  zip -qq -r -u ../terraform/weather_data_model.zip ./*
)
rm -rf build

echo "Terraform time!"
(
  cd terraform || exit
  terraform apply -auto-approve || exit 1
  rm -rf weather_etl.zip
  rm -rf weather_data_model.zip
)