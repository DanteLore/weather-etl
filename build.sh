echo "Running tests..."
pytest tests/ || exit 1

echo "Building the Weather ETL"
mkdir build || exit

cp datahub_etl/lambda_function.py build
cp datahub_etl/weather_etl.py build
cp datahub_etl/api_key.py build
cp datahub_etl/datahub_client.py build
cp datahub_etl/site_loader.py build
cp datahub_etl/sites.json build
cp datahub_etl/geohash_cache.json build
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
  terraform apply -auto-approve
  rm -rf weather_etl.zip
  rm -rf weather_data_model.zip
)