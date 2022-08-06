
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

resource "aws_s3_bucket" "audit_bucket" {
  bucket =  "${var.bucket_name}-audit"
  tags = var.tags
}

resource "aws_s3_bucket_logging" "bucket_to_audit_bucket" {
  bucket = aws_s3_bucket.bucket.id

  target_bucket = aws_s3_bucket.audit_bucket.id
  target_prefix = "s3_log/"
}

output "bucket_name" {
  value = aws_s3_bucket.bucket.bucket
}

output "bucket_arn" {
  value = aws_s3_bucket.bucket.arn
}
