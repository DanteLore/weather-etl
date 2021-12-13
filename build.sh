mkdir build

cp lambda_function.py build
cp api_key.py build
cp weather_etl.py build
cp aws_helpers.py build

(
  cd build || exit
  pip install --target . -r ../requirements.txt
  zip -r -u ../lambda.zip ./*
)
rm -rf build

terraform apply -auto-approve

rm -rf lambda.zip