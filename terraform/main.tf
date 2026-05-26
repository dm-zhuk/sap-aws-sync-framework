provider "aws" {
  region = var.aws_region
}

# Data Lake storage bucket
resource "aws_s3_bucket" "sap_lake" {
  bucket        = var.bucket_name
  force_destroy = true

  tags = {
    Environment = "Thesis-Simulation"
    Engine      = "Ghost-SAP-CDC"
  }
}

# Security layer: enable private access
resource "aws_s3_bucket_public_access_block" "security_gate" {
  bucket = aws_s3_bucket.sap_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

import {
  to = aws_s3_bucket.sap_lake
  id = "mcs-capstone-datalake-829703038395"
}