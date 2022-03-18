variable "name" {
  type = string
}

variable "tags" {
  type = map
  description = "Project tags"
}

variable "read_users_arns" {
  type = list(string)
  description = "IAM users who are authorized to read dvc/ objects"
  default = []
}

variable "write_user_arn" {
  type = string
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

# create a limited access policy for the project user
data "aws_iam_policy_document" "user_access_base" {
  for_each = toset(concat(var.read_users_arns, [aws_iam_role.batch_execution_role.arn]))

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
  
}

data "aws_iam_policy_document" "user_access_process" {
  override_policy_documents = [for s in data.aws_iam_policy_document.user_access_base : s.json]

  statement {
    sid = "writedvc"
    principals {
      type = "AWS"
      identifiers = [var.write_user_arn]
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
      identifiers = [var.write_user_arn]
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

resource "aws_iam_policy" "policy" {
  name        = "${var.name}-batch-access-policy"
  policy      = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "",
            "Effect": "Allow",
            "Action": "batch:SubmitJob",
            "Resource": [
                "arn:aws:batch:eu-west-1:272181418418:job-queue/transfermarkt-datasets-batch-compute-job-queue",
                "arn:aws:batch:eu-west-1:272181418418:job-definition/transfermarkt-datasets-batch-job-definition"
            ]
        },
        {
            "Sid": "",
            "Effect": "Allow",
            "Action": [
                "batch:DescribeJobDefinitions"
            ],
            "Resource": "*"
        }
    ]
}
  EOF
}

resource "aws_iam_role" "batch_service_role" {
  name = "${var.name}-batch-service-role"
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "batch.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF

}


resource "aws_iam_role" "batch_execution_role" {
  name = "${var.name}-batch-execution-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      },
    ]
  })
  inline_policy {
    name = "s3access"

    policy = jsonencode({
      Version = "2012-10-17"
      Statement = [
        {
          Action   = [
            "s3:PutObject",
            "s3:GetObject"
          ]
          Effect   = "Allow"
          Resource = "${data.aws_s3_bucket.bucket.arn}/dvc/*"
        },
      ]
    })
  }
}

resource "aws_iam_role_policy_attachment" "batch_execution_role_attach" {
  role       = aws_iam_role.batch_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess"

}

resource "aws_iam_policy_attachment" "batch_service_role_attach" {
  name       = "${var.name}-batch-service-role-attach"
  roles      = [aws_iam_role.batch_service_role.name]
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole"
}


resource "aws_s3_bucket_policy" "bucket_policy_process" {
  bucket = data.aws_s3_bucket.bucket.id
  policy = data.aws_iam_policy_document.user_access_process.json
}

output "batch_service_role_arn" {
  value = aws_iam_role.batch_service_role.arn
}

output "batch_execution_role_arn" {
  value = aws_iam_role.batch_execution_role.arn
}
