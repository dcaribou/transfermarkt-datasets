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
  dvc_read_users = [
    "arn:aws:iam::272181418418:user/transfermarkt-datasets"
  ]
}

module "base" {
  source = "./base"
  bucket_name = "transfermarkt-datasets"
  tags = {
    "project" = "transfermarkt-datasets"
  }
}

module "iam" {
  name = "transfermarkt-datasets"
  source = "./iam"
  read_users_arns = local.dvc_read_users
  write_user_arn = "arn:aws:iam::272181418418:user/transfermarkt-datasets"
  bucket_name = module.base.bucket_name
  bucket_arn = module.base.bucket_arn
  tags = {
    "project" = "transfermarkt-datasets"
  }
}

module "batch" {
  name = "transfermarkt-datasets"
  source = "./batch"
  execution_role_arn = module.iam.batch_execution_role_arn
  service_role_arn = module.iam.batch_service_role_arn
}
