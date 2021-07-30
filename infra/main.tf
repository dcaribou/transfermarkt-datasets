terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "3.52.0"
    }
  }
}

provider "aws" {
  region = "eu-west-1"
}

# add your AWS IAM user ARN to this list to gain access to DVC remote storage
locals {
  authorized_users = [
  ]
}
module "base" {
  source = "./base"
  bucket_name = "player-scores"
  user_name = "player-scores"
  authorized_users = local.authorized_users
  tags = {
    "project" = "player-scores"
  }
}
