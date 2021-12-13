provider "aws" {
  region = "eu-west-1"
}

variable "function_name" {
  default = "load_weather_data"
}

variable "handler" {
  default = "lambda.handler"
}

variable "runtime" {
  default = "python3.6"
}

resource "aws_lambda_function" "lambda_function" {
  role             = aws_iam_role.lambda_exec_role.arn
  handler          = var.handler
  runtime          = var.runtime
  filename         = "lambda.zip"
  function_name    = var.function_name
  source_code_hash = filebase64sha256("lambda.zip")
  timeout          = 60
}

resource "aws_cloudwatch_event_rule" "time_to_load_the_data" {
  name                = "time_to_load_the_data"
  description         = "Grab the data just after midnight"
  schedule_expression = "cron(10 0 * * ? *)"
}

resource "aws_cloudwatch_event_target" "load_data_at_half_past_midnight" {
  rule      = aws_cloudwatch_event_rule.time_to_load_the_data.name
  target_id = "lambda"
  arn       = aws_lambda_function.lambda_function.arn
}

resource "aws_lambda_permission" "allow_cloudwatch_to_call_lambda" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_function.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.time_to_load_the_data.arn
}

resource "aws_iam_role" "lambda_exec_role" {
  name        = "execute_weather_data_lambda"
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
  name        = "weather_data_policy"
  path        = "/"
  description = "TBD"

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "s3:GetObject*",
        "s3:ListBucket*",
        "s3:PutObject*",
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": [
          "*"
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
  name = "lambda_profile"
  role = aws_iam_role.lambda_exec_role.name
}