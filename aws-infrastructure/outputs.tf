output "data_lake_bucket_name" {
  value       = aws_s3_bucket.sap_lake.id
  description = "The verified bucket name deployed on AWS"
}