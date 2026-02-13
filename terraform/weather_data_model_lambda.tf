variable "weather_data_model_lambda" {
  default = "model_weather_data"
}

variable "weather_data_model_handler" {
  default = "lambda_function.handler"
}

variable "weather_data_model_cloudwatch_event" {
  default = "time_to_model_weather_data"
}

resource "aws_cloudwatch_log_group" "weather_data_model_loggroup" {
  name              = "/aws/lambda/${var.weather_data_model_lambda}"
  retention_in_days = 3
}

resource "aws_lambda_function" "weather_data_model_lambda_function" {
  role             = aws_iam_role.weather_data_model_lambda_exec_role.arn
  handler          = var.weather_data_model_handler
  runtime          = var.runtime
  filename         = "weather_data_model.zip"
  function_name    = var.weather_data_model_lambda
  source_code_hash = filebase64sha256("weather_data_model.zip")
  timeout          = 60
}

resource "aws_cloudwatch_event_rule" "time_to_model_weather_data" {
  name                = var.weather_data_model_cloudwatch_event
  description         = "Process the data just before work starts"
  schedule_expression = "cron(0 6 * * ? *)"
}

resource "aws_cloudwatch_event_target" "model_data_in_the_morning" {
  rule      = aws_cloudwatch_event_rule.time_to_model_weather_data.name
  target_id = "lambda"
  arn       = aws_lambda_function.weather_data_model_lambda_function.arn
}

resource "aws_lambda_permission" "weather_data_model_allow_cloudwatch_to_call_lambda" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.weather_data_model_lambda_function.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.time_to_model_weather_data.arn
}

resource "aws_iam_role" "weather_data_model_lambda_exec_role" {
  name        = "execute_weather_data_model_lambda"
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

resource "aws_iam_policy" "weather_data_model_lambda_policy" {
  name        = "weather_data_model_policy"
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
      ],
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
          "arn:aws:s3:::dantelore.data.lake",
          "arn:aws:s3:::dantelore.data.lake/*",
          "arn:aws:s3:::dantelore.queryresults",
          "arn:aws:s3:::dantelore.queryresults/*"
      ]
      ,
      "Effect": "Allow",
      "Sid": ""
    },
    {
      "Action": [
        "s3:DeleteObject"
      ],
      "Resource": [
          "arn:aws:s3:::dantelore.data.lake/weather/*",
          "arn:aws:s3:::dantelore.data.lake/weather_monthly_site_summary/*"
      ],
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "weather_data_model_server_policy" {
  role       = aws_iam_role.weather_data_model_lambda_exec_role.name
  policy_arn = aws_iam_policy.weather_data_model_lambda_policy.arn
}

resource "aws_iam_instance_profile" "weather_data_model_server" {
  name = "weather_data_model_lambda_profile"
  role = aws_iam_role.weather_data_model_lambda_exec_role.name
}