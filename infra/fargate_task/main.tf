# https://particule.io/en/blog/cicd-ecr-ecs/

variable "name" {
  type = string
}

variable "image" {
  type = string
}

variable "tags" {
  type = map
  description = "Project tags"
}

locals {
    cloudwatch_log_group_name = "${var.name}-fargate-task"
}


data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

resource "aws_ecs_cluster" "cluster" {
  name               = var.name
  capacity_providers = ["FARGATE"]

  default_capacity_provider_strategy {
    capacity_provider = "FARGATE"
    weight            = "100"
  }
}

resource "aws_iam_role" "fargate" {
  name = var.name
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = [
            "ecs.amazonaws.com",
            "ecs-tasks.amazonaws.com"
          ]
        }
      },
    ]
  })
}

resource "aws_iam_role_policy" "fargate" {
  name = var.name
  role = aws_iam_role.fargate.id

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "ecr:CompleteLayerUpload",
        "ecr:DescribeRepositories",
        "ecr:ListImages",
        "ecr:DescribeImages",
        "ecr:GetAuthorizationToken",
        "ecr:GetDownloadUrlForLayer",
        "ecr:GetLifecyclePolicy",
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:DescribeLogStreams"
      ],
      "Effect": "Allow",
      "Resource": "*"
    }
  ]
}
EOF
}

resource "aws_cloudwatch_log_group" "cloudwatch_group" {
  name = local.cloudwatch_log_group_name
}

resource "aws_ecs_task_definition" "task" {
  family = var.name
  requires_compatibilities = [
    "FARGATE",
  ]
  execution_role_arn = aws_iam_role.fargate.arn
  network_mode       = "awsvpc"
  cpu                = 256
  memory             = 512
  container_definitions = jsonencode([
    {
        name      = var.name
        image     = var.image
        essential = true
        portMappings = [
            {
                containerPort = 80
                hostPort      = 80
            }
        ]
        logConfiguration = {
            logDriver = "awslogs"
            options = {
                awslogs-group = "${local.cloudwatch_log_group_name}"
                awslogs-region = "${data.aws_region.current.name}"
                awslogs-stream-prefix = "main"
            }
        }
        command = ["ls"]
    }
  ])
}
