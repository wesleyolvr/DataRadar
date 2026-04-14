# S3 Bucket para armazenamento Bronze (raw data)
resource "aws_s3_bucket" "raw_data" {
  bucket = var.s3_bucket_name

  tags = merge(
    local.common_tags,
    {
      Name = "${local.resource_prefix}-raw-data"
      Layer = "Bronze"
    }
  )
}

# Versionamento do bucket
resource "aws_s3_bucket_versioning" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Lifecycle policy - mover dados antigos para Glacier após 90 dias
resource "aws_s3_bucket_lifecycle_configuration" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  rule {
    id     = "archive-old-data"
    status = "Enabled"

    filter {
      prefix = "reddit/"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    expiration {
      days = 365  # Delete após 1 ano
    }
  }
}

# Event notification para Lambda
resource "aws_s3_bucket_notification" "raw_data_events" {
  bucket = aws_s3_bucket.raw_data.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.s3_trigger.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "reddit/"
    filter_suffix       = "raw_"
  }

  depends_on = [aws_lambda_permission.allow_s3]
}

# Block public access
resource "aws_s3_bucket_public_access_block" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
