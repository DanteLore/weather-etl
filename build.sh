mkdir build

cp datapoint_etl/* build
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