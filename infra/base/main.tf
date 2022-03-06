# create an S3 bucket for the project files
resource "aws_s3_bucket" "bucket" {
  bucket = var.bucket_name
  tags = var.tags
}

# create a user for the automations
resource "aws_iam_user" "user" {
  name = var.user_name
  tags = var.tags
}

output "bucket_name" {
  value = aws_s3_bucket.bucket.bucket
}

output "bucket_arn" {
  value = aws_s3_bucket.bucket.arn
}
