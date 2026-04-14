variable "environment" {
  description = "Environment name (dev or prod)"
  type        = string
  default     = "prod"

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "Environment must be 'dev' or 'prod'."
  }
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "devradar"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "s3_bucket_name" {
  description = "S3 bucket for raw data storage"
  type        = string
  default     = "devradar-raw"
}

variable "lambda_function_name" {
  description = "Lambda function name"
  type        = string
  default     = "devradar-s3-trigger"
}

variable "databricks_host" {
  description = "Databricks workspace URL (without https://)"
  type        = string
  sensitive   = true
}

variable "databricks_token" {
  description = "Databricks personal access token"
  type        = string
  sensitive   = true
}

variable "databricks_job_id" {
  description = "Databricks job ID to trigger"
  type        = string
}

variable "groq_api_key" {
  description = "Groq API key for LLM insights"
  type        = string
  sensitive   = true
}
