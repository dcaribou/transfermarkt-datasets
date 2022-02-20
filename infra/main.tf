terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "3.52.0"
    }
  }
  required_version = "1.0.4"
}

provider "aws" {
  region = "eu-west-1"
}

# add your AWS IAM user ARN to this list to gain access to DVC remote storage
locals {
  authorized_users = [
    "arn:aws:iam::272181418418:user/transfermarkt-datasets"
  ]
}
module "base" {
  source = "./base"
  bucket_name = "transfermarkt-datasets"
  user_name = "transfermarkt-datasets"
  authorized_users = local.authorized_users
  tags = {
    "project" = "transfermarkt-datasets"
  }
}

module "task" {
  source = "./fargate_task"
  name = "transfermarkt-datasets"
  image = "dcaribou/transfermarkt-datasets"
  tags = {
    "project" = "transfermarkt-datasets"
  }
}
