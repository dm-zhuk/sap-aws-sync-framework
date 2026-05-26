variable "aws_region" {
  type        = string
  default     = "eu-central-1"
  description = "The target AWS region for cloud data lake deployment"
}

variable "bucket_name" {
  type        = string
  default     = "mcs-capstone-datalake-829703038395"
  description = "S3 bucket for landing SAP transactions"
}