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

locals {
  core_bucket_name = "transfermarkt-datasets"
  # write access is only granted via S3 direct access
  dvc_read_write_users = [
    "arn:aws:iam::272181418418:user/transfermarkt-datasets",
  ]
  module_tags = {
    "project" = "transfermarkt-datasets"
  }
}

module "base" {
  source = "./base"
  bucket_name = local.core_bucket_name
  tags = local.module_tags
}

module "cdn" {
  source = "./cdn"
  name = "transfermarkt-datasets"
  bucket_name = module.base.bucket_name
  tags = local.module_tags
}

module "iam" {
  name = "transfermarkt-datasets"
  source = "./iam"
  read_write_users_arns = local.dvc_read_write_users
  bucket_name = module.base.bucket_name
  bucket_arn = module.base.bucket_arn
  cdn_arn = module.cdn.arn
  tags = local.module_tags
}
