terraform {
  required_providers {
    cloudflare = {
      source = "cloudflare/cloudflare"
    }
  }
}

variable "bucket_name" {
  type        = string
  description = "The name of the R2 bucket"
}

variable "account_id" {
  type        = string
  description = "Cloudflare account ID"
}

resource "cloudflare_r2_bucket" "bucket_public" {
  account_id = var.account_id
  name       = "${var.bucket_name}-public"
  location   = "WEUR"
}
