provider "aws" {
  region = "eu-west-1"
}

terraform {
  backend "s3" {
    # Replace this with your bucket name!
    bucket         = "dantelore.tfstate"
    key            = "weather.tfstate"
    region         = "eu-west-1"
  }
}

variable "function_name" {
  default = "load_weather_data"
}

variable "handler" {
  default = "lambda_function.handler"
}

variable "runtime" {
  default = "python3.9"
}

resource "aws_cloudwatch_log_group" "loggroup" {
  name              = "/aws/lambda/${var.function_name}"
  retention_in_days = 3
}

resource "aws_lambda_function" "lambda_function" {
  role             = aws_iam_role.lambda_exec_role.arn
  handler          = var.handler
  runtime          = var.runtime
  filename         = "weather_etl.zip"
  function_name    = var.function_name
  source_code_hash = filebase64sha256("weather_etl.zip")
  timeout          = 60
}

resource "aws_cloudwatch_event_rule" "time_to_load_weather_data" {
  name                = "time_to_load_weather_data"
  description         = "Grab the data just before midnight"
  schedule_expression = "cron(50 23 * * ? *)"
}

resource "aws_cloudwatch_event_target" "load_data_at_half_past_midnight" {
  rule      = aws_cloudwatch_event_rule.time_to_load_weather_data.name
  target_id = "lambda"
  arn       = aws_lambda_function.lambda_function.arn
}

resource "aws_lambda_permission" "allow_cloudwatch_to_call_lambda" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_function.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.time_to_load_weather_data.arn
}

resource "aws_iam_role" "lambda_exec_role" {
  name        = "execute_weather_etl_lambda"
  path        = "/"
  description = "IAM role for the Weather Data lambda function"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_iam_policy" "lambda_policy" {
  name        = "weather_etl_policy"
  path        = "/"

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:CreatePartition",
        "glue:BatchCreatePartition",
        "glue:UpdatePartition",
        "glue:GetPartition",
        "glue:GetPartitions"
      ],
      "Resource": [
          "*"
      ]
      ,
      "Effect": "Allow",
      "Sid": ""
    },
    {
      "Action": [
        "s3:GetObject*",
        "s3:ListBucket*",
        "s3:PutObject*",
        "s3:GetBucketLocation",
        "s3:ListMultipartUploadParts",
        "s3:AbortMultipartUpload",
        "s3:CreateBucket",
        "s3:PutObject"
      ],
      "Resource": [
          "arn:aws:s3:::dantelore.data.incoming",
          "arn:aws:s3:::dantelore.data.incoming/*",
          "arn:aws:s3:::dantelore.data.raw",
          "arn:aws:s3:::dantelore.data.raw/*",
          "arn:aws:s3:::dantelore.queryresults",
          "arn:aws:s3:::dantelore.queryresults/*"
      ]
      ,
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "server_policy" {
  role       = aws_iam_role.lambda_exec_role.name
  policy_arn = aws_iam_policy.lambda_policy.arn
}

resource "aws_iam_instance_profile" "server" {
  name = "weather_etl_lambda_profile"
  role = aws_iam_role.lambda_exec_role.name
}