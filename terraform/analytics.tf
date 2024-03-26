################################################################################
# GeoIP Database
# TODO: remove from state and re-attach to data stack

module "geo_ip_database_bucket" {
  source = "github.com/WalletConnect/terraform-modules/modules/s3"

  application = "${local.app_name}.geo.ip.database"
  env         = terraform.workspace
  env_group   = terraform.workspace
  tags        = {} # We're using default tags in this repo
  acl         = "private"
  versioning  = false

  providers = {
    aws = aws.eu-central-1
  }
}



################################################################################
# Legacy Data Lake bucket
# TODO: remove from state / delete when migration is complete.

locals {
  legacy_analytics_bucket_name = "walletconnect.ts-${local.app_name}.${terraform.workspace}.analytics.data-lake"
}

resource "aws_s3_bucket" "analytics_data_lake-bucket" {
  bucket = local.legacy_analytics_bucket_name
}

resource "aws_s3_bucket_acl" "analytics_data_lake-acl" {
  bucket = aws_s3_bucket.analytics_data_lake-bucket.id
  acl    = "private"
}

resource "aws_s3_bucket_public_access_block" "analytics_data_lake-bucket" {
  bucket = aws_s3_bucket.analytics_data_lake-bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_kms_key" "analytics_data_lake-bucket" {
  description             = "${local.app_name}.${terraform.workspace} - analytics bucket encryption"
  enable_key_rotation     = true
  deletion_window_in_days = 10
}

resource "aws_kms_alias" "analytics_data_lake-bucket" {
  target_key_id = aws_kms_key.analytics_data_lake-bucket.id
  name          = "alias/analytics/${local.app_name}/${terraform.workspace}"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "analytics_data_lake-bucket" {
  bucket = aws_s3_bucket.analytics_data_lake-bucket.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.analytics_data_lake-bucket.arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_versioning" "analytics_data_lake-bucket" {
  bucket = aws_s3_bucket.analytics_data_lake-bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}
