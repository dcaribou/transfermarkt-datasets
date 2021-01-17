# create an S3 bucket for the project files
resource "aws_s3_bucket" "bucket" {
  bucket = var.bucket_name
}

# create a user for the automations
resource "aws_iam_user" "user" {
  name = var.user_name
}

# create a limited access policy for the project user
data "aws_iam_policy_document" "user_access" {
  statement {
    sid = "bucket-level"
    principals {
      type = "AWS"
      identifiers = [aws_iam_user.user.arn]
    }
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket"
    ]

    resources = [
      aws_s3_bucket.bucket.arn
    ]
  }

  statement {
    sid = "read-write"
    principals {
      type = "AWS"
      identifiers = [aws_iam_user.user.arn]
    }
    actions = [
      "s3:GetObject",
      "s3:PutObject"
    ]

    resources = [
      "${aws_s3_bucket.bucket.arn}/*",
    ]
  } 
  
  statement {
    sid = "delete"
    principals {
      type = "AWS"
      identifiers = [aws_iam_user.user.arn]
    }
    actions = [
      "s3:DeleteObject"
    ]

    resources = [
      "${aws_s3_bucket.bucket.arn}/snapshots/*",
    ]
  }
}

# attach policy to the bucket
resource "aws_s3_bucket_policy" "bucket_policy" {
  bucket = aws_s3_bucket.bucket.id
  policy = data.aws_iam_policy_document.user_access.json
}
