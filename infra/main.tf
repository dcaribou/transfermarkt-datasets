terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = ">= 4.19.0"
    }
  }
  required_version = "1.5.7"
}

provider "aws" {
  region = "eu-west-1"
}

# write access is only granted via S3 direct access
locals {
  dvc_read_write_users = [
    "arn:aws:iam::272181418418:user/transfermarkt-datasets",
  ]
}

module "base" {
  source = "./base"
  bucket_name = "transfermarkt-datasets"
  tags = {
    "project" = "transfermarkt-datasets"
  }
}

module "cdn" {
  source = "./cdn"
  name = "transfermarkt-datasets"
  bucket_name = module.base.bucket_name
  tags = {
    "project" = "transfermarkt-datasets"
  }
}

module "iam" {
  name = "transfermarkt-datasets"
  source = "./iam"
  read_write_users_arns = local.dvc_read_write_users
  bucket_name = module.base.bucket_name
  bucket_arn = module.base.bucket_arn
  cdn_arn = module.cdn.arn
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


