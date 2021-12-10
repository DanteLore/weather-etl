# Weather ETL

The simplest, most basic implementation of an ETL in AWS.  Pulls data fro the Met Office
[DataPoint API](https://www.metoffice.gov.uk/services/data/datapoint), 
which provides 24 hourly weather observations in real-ish time.

Data is currently pulled by a Python ETL, within a Lambda function, triggered just 
before midnight each night.  Output is pushed to S3 in a more queryable JSON format.

* **weather_etl** the code that does the transform/cleaning of the data
* **main.py** run it locally
* **lambda.py** the lambda function implementation
* **lambda.tf** terraform to create the lambda, roles, trigger etc
