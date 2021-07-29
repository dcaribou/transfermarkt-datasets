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

# create a limited access policy for the project user
data "aws_iam_policy_document" "user_access_base" {
  for_each = toset(var.authorized_users)

  statement {
    sid = "bucket-level"
    principals {
      type = "AWS"
      identifiers = [each.key]
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
    sid = "read-dvc"
    principals {
      type = "AWS"
      identifiers = [aws_iam_user.user.arn]
    }
    actions = [
      "s3:GetObject"
    ]

    resources = [
      "${aws_s3_bucket.bucket.arn}/dvc/*",
    ]
  } 
  
  
}

data "aws_iam_policy_document" "user_access_process" {
  statement {
    sid = "write-dvc"
    principals {
      type = "AWS"
      identifiers = [aws_iam_user.user.arn]
    }
    actions = [
      "s3:PutObject",
      "s3:GetObject"
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
resource "aws_s3_bucket_policy" "bucket_policy_base" {
  for_each = toset([for s in data.aws_iam_policy_document.user_access_base : s.json])

  bucket = aws_s3_bucket.bucket.id
  policy = each.key
}

resource "aws_s3_bucket_policy" "bucket_policy_process" {
  bucket = aws_s3_bucket.bucket.id
  policy = data.aws_iam_policy_document.user_access_process.json
}

output "process_user_arn" {
  value = aws_iam_user.user.arn
}
