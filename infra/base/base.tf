
variable "bucket_name" {
  type = string
  description = "The name of the base bucket for the project"
}

variable "tags" {
  type = map
  description = "Project tags"
}

# create an S3 bucket for the project files
resource "aws_s3_bucket" "bucket" {
  bucket = var.bucket_name
  tags = var.tags
}

output "bucket_name" {
  value = aws_s3_bucket.bucket.bucket
}

output "bucket_arn" {
  value = aws_s3_bucket.bucket.arn
}
