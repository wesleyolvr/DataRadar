# Lambda function para trigger Databricks
resource "aws_lambda_function" "s3_trigger" {
  filename      = "${path.module}/../lambda/handler.zip"
  function_name = "${local.resource_prefix}-${var.lambda_function_name}"
  role          = aws_iam_role.lambda_execution.arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.11"
  timeout       = 30
  memory_size   = 256

  source_code_hash = fileexists("${path.module}/../lambda/handler.zip") ? filebase64sha256("${path.module}/../lambda/handler.zip") : null

  environment {
    variables = {
      ENVIRONMENT        = var.environment
      DATABRICKS_DOMAIN  = var.databricks_host
      DATABRICKS_TOKEN   = var.databricks_token
      JOB_ID             = var.databricks_job_id
    }
  }

  tags = merge(
    local.common_tags,
    {
      Name = "${local.resource_prefix}-databricks-trigger"
    }
  )
}

# Permission para S3 invocar Lambda
resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowExecutionFromS3"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.s3_trigger.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.raw_data.arn
}

# CloudWatch Log Group para Lambda
resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${aws_lambda_function.s3_trigger.function_name}"
  retention_in_days = 14

  tags = local.common_tags
}
