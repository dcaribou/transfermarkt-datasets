terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "3.24.1"
    }
  }
}

provider "aws" {
  region = "eu-west-1"
}

module "base" {
  source = "./base"
  bucket_name = "player-scores"
  user_name = "player-scores"
  authorized_users = [
    "arn:aws:iam::272181418418:user/player-scores"
  ]
  tags = {
    "project" = "player-scores"
  }
}
