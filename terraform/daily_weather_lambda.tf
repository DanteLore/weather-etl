variable "datapoint_etl_function_name" {
  default = "load_weather_data"
}

variable "datapoint_etl_handler" {
  default = "datahub_etl.lambda_function.handler"
}

variable "datapoint_etl_cloudwatch_event" {
  default = "time_to_load_weather_data"
}

resource "aws_cloudwatch_log_group" "datapoint_etl_loggroup" {
  name              = "/aws/lambda/${var.datapoint_etl_function_name}"
  retention_in_days = 3
}

resource "aws_lambda_function" "datapoint_etl_lambda_function" {
  role             = aws_iam_role.datapoint_etl_lambda_exec_role.arn
  handler          = var.datapoint_etl_handler
  runtime          = var.runtime
  filename         = "weather_etl.zip"
  function_name    = var.datapoint_etl_function_name
  source_code_hash = filebase64sha256("weather_etl.zip")
  timeout          = 300
}

resource "aws_cloudwatch_event_rule" "time_to_load_weather_data" {
  name                = var.datapoint_etl_cloudwatch_event
  description         = "Fetch weather data hourly in batches"
  schedule_expression = "rate(1 hour)"
}

resource "aws_cloudwatch_event_target" "load_data_at_half_past_midnight" {
  rule      = aws_cloudwatch_event_rule.time_to_load_weather_data.name
  target_id = "lambda"
  arn       = aws_lambda_function.datapoint_etl_lambda_function.arn
}

resource "aws_lambda_permission" "datapoint_etl_allow_cloudwatch_to_call_lambda" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.datapoint_etl_lambda_function.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.time_to_load_weather_data.arn
}

resource "aws_iam_role" "datapoint_etl_lambda_exec_role" {
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

resource "aws_iam_policy" "datapoint_etl_lambda_policy" {
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
          "arn:aws:s3:::dantelore.data.lake",
          "arn:aws:s3:::dantelore.data.lake/*",
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

resource "aws_iam_role_policy_attachment" "datapoint_etl_server_policy" {
  role       = aws_iam_role.datapoint_etl_lambda_exec_role.name
  policy_arn = aws_iam_policy.datapoint_etl_lambda_policy.arn
}

resource "aws_iam_instance_profile" "datapoint_etl_server" {
  name = "weather_etl_lambda_profile"
  role = aws_iam_role.datapoint_etl_lambda_exec_role.name
}