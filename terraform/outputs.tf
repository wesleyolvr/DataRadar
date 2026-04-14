output "s3_bucket_name" {
  description = "Nome do bucket S3 criado"
  value       = aws_s3_bucket.raw_data.id
}

output "s3_bucket_arn" {
  description = "ARN do bucket S3"
  value       = aws_s3_bucket.raw_data.arn
}

output "lambda_function_name" {
  description = "Nome da função Lambda"
  value       = aws_lambda_function.s3_trigger.function_name
}

output "lambda_function_arn" {
  description = "ARN da função Lambda"
  value       = aws_lambda_function.s3_trigger.arn
}

output "lambda_role_arn" {
  description = "ARN do IAM role da Lambda"
  value       = aws_iam_role.lambda_execution.arn
}

output "ssm_parameter_paths" {
  description = "Paths dos parâmetros SSM criados"
  value = {
    databricks_host   = aws_ssm_parameter.databricks_host.name
    databricks_token  = aws_ssm_parameter.databricks_token.name
    databricks_job_id = aws_ssm_parameter.databricks_job_id.name
    groq_api_key      = aws_ssm_parameter.groq_api_key.name
  }
}

output "environment" {
  description = "Environment deployed"
  value       = var.environment
}
