mkdir build

cp lambda.py build
cp api_key build
cp main.py build

(
  cd build
  pip install --target . -r ../requirements.txt
  zip -r ../lambda.zip *
)
rm -rf build

terraform apply -auto-approve

rm -rf lambda.zip