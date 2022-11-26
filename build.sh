mkdir build

cp datapoint_etl/lambda_function.py build
cp datapoint_etl/weather_etl.py build
cp datapoint_etl/api_key.py build
cp datapoint_etl/weather_schema.json build
cp -Rf helpers build

(
  cd build || exit
  pip install --target . -r ../requirements.txt
  zip -r -u ../terraform/weather_etl.zip ./*
)
rm -rf build

(
  cd terraform || exit
  terraform apply -auto-approve
  rm -rf weather_etl.zip
)