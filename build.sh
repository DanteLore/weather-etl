mkdir build

cp lambda.py build
cp api_key.py build
cp weather_etl.py build

(
  cd build || exit
  pip install --target . -r ../requirements.txt
  zip -r ../lambda.zip ./*
)
rm -rf build

terraform apply -auto-approve

rm -rf lambda.zip