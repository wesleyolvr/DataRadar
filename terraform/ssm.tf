# AWS Systems Manager Parameter Store - Secrets centralizados
# Os valores serão injetados via terraform.tfvars ou variáveis de ambiente

resource "aws_ssm_parameter" "databricks_host" {
  name  = "/${var.project_name}/${var.environment}/databricks_host"
  type  = "SecureString"
  value = var.databricks_host
}

resource "aws_ssm_parameter" "databricks_token" {
  name  = "/${var.project_name}/${var.environment}/databricks_token"
  type  = "SecureString"
  value = var.databricks_token
}

resource "aws_ssm_parameter" "databricks_job_id" {
  name  = "/${var.project_name}/${var.environment}/databricks_job_id"
  type  = "String"
  value = var.databricks_job_id
}

resource "aws_ssm_parameter" "groq_api_key" {
  name  = "/${var.project_name}/${var.environment}/groq_api_key"
  type  = "SecureString"
  value = var.groq_api_key
}
