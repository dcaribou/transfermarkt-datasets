variable "name" {
  type = string
}

variable "tags" {
  type = map
  description = "Project tags"
}

variable "read_write_users_arns" {
  type = list(string)
  description = "IAM users who are authorized to read dvc/ objects"
  default = []
}

variable "cdn_arn" {
  type = string
  description = "ARN of the Cloudfront distribution to be used as DVC remote"
}

variable "bucket_name" {
  type = string
}
variable "bucket_arn" {
  type = string
}

data "aws_s3_bucket" "bucket" {
  bucket = var.bucket_name
}

# create policy with the read grants
data "aws_iam_policy_document" "user_access_base" {
  for_each = toset(var.read_write_users_arns)

  statement {
    sid = "bucketlevel${sha256(each.key)}"
    principals {
      type = "AWS"
      identifiers = [each.key]
    }
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket"
    ]

    resources = [
      data.aws_s3_bucket.bucket.arn
    ]
  }

  statement {
    sid = "readdvc${sha256(each.key)}"
    principals {
      type = "AWS"
      identifiers = [each.key]
    }
    actions = [
      "s3:GetObject"
    ]

    resources = [
      "${data.aws_s3_bucket.bucket.arn}/dvc/*",
    ]
  }

  statement {
    sid = "cloudfront"
    principals {
      type = "Service"
      identifiers = ["cloudfront.amazonaws.com"]
    }
    actions = [
      "s3:GetObject"
    ]
    condition {
      test = "StringEquals"
      variable = "AWS:SourceArn"
      values = [var.cdn_arn]
    }
    resources = [ 
      "${data.aws_s3_bucket.bucket.arn}/dvc/*",
    ]
  }
  
}

# add write grants to the read policy (by overriding / enhancing the read grants)
data "aws_iam_policy_document" "user_access_process" {
  override_policy_documents = [for s in data.aws_iam_policy_document.user_access_base : s.json]

  statement {
    sid = "writedvc"
    principals {
      type = "AWS"
      identifiers = var.read_write_users_arns
    }
    actions = [
      "s3:PutObject",
      "s3:GetObject"
    ]

    resources = [
      "${data.aws_s3_bucket.bucket.arn}/*",
    ]
  }

  statement {
    sid = "delete"
    principals {
      type = "AWS"
      identifiers = var.read_write_users_arns
    }
    actions = [
      "s3:DeleteObject"
    ]

    resources = [
      "${data.aws_s3_bucket.bucket.arn}/snapshots/*",
    ]
  }

}

resource "aws_iam_user" "user" {
  name = var.name
  tags = var.tags
}

resource "aws_iam_user" "user_streamlit" {
  name = "${var.name}-streamlit"
  tags = var.tags
}

resource "aws_s3_bucket_policy" "bucket_policy_process" {
  bucket = data.aws_s3_bucket.bucket.id
  policy = data.aws_iam_policy_document.user_access_process.json
}
