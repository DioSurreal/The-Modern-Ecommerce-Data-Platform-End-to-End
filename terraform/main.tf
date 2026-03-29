terraform {
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.87.0" 
    }
  }
}

provider "snowflake" {
  account  = "EZHYYIM-YD05486"  
  username = var.snowflake_username
  password = var.snowflake_password
  role     = "ACCOUNTADMIN"
}

resource "snowflake_warehouse" "ecommerce_wh" {
  name           = "ECOMMERCE_WH"
  warehouse_size = "XSMALL" 
  auto_suspend   = 60       
  auto_resume    = true
}

resource "snowflake_database" "raw_db" {
  name = "ECOMMERCE_RAW"  
}

resource "snowflake_database" "prod_db" {
  name = "ECOMMERCE_PROD" 
}

resource "snowflake_role" "loader_role" {
  name = "ECOMMERCE_LOADER"
}
provider "aws" {
  region = "ap-southeast-1" 
}

resource "aws_s3_bucket" "ecommerce_landing_zone" {
  bucket = "my-ecommerce-project-landing-zone" 
}

resource "aws_iam_role" "snowflake_role" {
  name = "snowflake_s3_access_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::487442499778:user/ojrl1000-s"
        }
        Condition = {
          StringEquals = {
            "sts:ExternalId" = "IH62328_SFCRole=6_uXusBqq+WEfiDzOXjh0rdk4GhS0="
          }
        }
      }
    ]
  })
}

data "aws_caller_identity" "current" {}

resource "aws_iam_policy" "s3_policy" {
  name        = "SnowflakeS3AccessPolicy"
  description = "Allow Snowflake to read/write to Ecommerce S3"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
        Effect   = "Allow"
        Resource = [
            "${aws_s3_bucket.ecommerce_landing_zone.arn}",
            "${aws_s3_bucket.ecommerce_landing_zone.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_policy" {
  role       = aws_iam_role.snowflake_role.name
  policy_arn = aws_iam_policy.s3_policy.arn
}

resource "snowflake_storage_integration" "s3_integration" {
  name    = "S3_INTEGRATION"
  comment = "Storage Integration for Ecommerce Project"
  type    = "EXTERNAL_STAGE"

  enabled = true

  storage_aws_role_arn     = aws_iam_role.snowflake_role.arn
  storage_allowed_locations = ["s3://${aws_s3_bucket.ecommerce_landing_zone.bucket}/"]
  storage_provider         = "S3"
}