# Weather ETL

The simplest, most basic implementation of an ETL in AWS.  Pulls data from the Met Office
[DataPoint API](https://www.metoffice.gov.uk/services/data/datapoint), 
which provides 24 hourly weather observations in real-ish time.

Data is currently pulled by a Python ETL, within a Lambda function, triggered just 
before midnight each night.  Output is pushed to S3 in a more queryable JSON format.

* **weather_etl** the code that does the transform/cleaning of the data
* **main.py** run it locally
* **lambda_function.py** the lambda function implementation and AWS specific stuff
* **lambda_function.tf** terraform to create the lambda, roles, trigger etc
* **glue.tf** setup for the glue table, including schema, partitioning etc

# Thoughts

* Would it be better to pull the raw data direct from source and store it to S3 "acquired" then process to the cleaner form?  Safer from bugs in the transform/decode code then
* Unit tests are nice, should have some ;)
* How to deal with failures - what happens if the job fails - only 24h of data is available on the API, so failure would mean data loss.  How to deal with this IRL?
* Check validity of data, handle bugs/bad data etc elegantly
* Schema-on-write validation