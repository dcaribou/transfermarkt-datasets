variable "bucket_name" {
  type = string
  description = "The name of the base bucket for the project"
}

variable "name" {
  type = string
  description = "Identify the set of resources created in this module."
}

variable "tags" {
  type = map
  description = "Project tags"
}

locals {
  s3_origin_id = "${var.bucket_name}.s3.eu-west-1.amazonaws.com"
}

resource "aws_cloudfront_origin_access_control" "oac" {
  name                              = var.name
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

resource "aws_cloudfront_distribution" "s3_distribution" {
  origin {
    domain_name              = local.s3_origin_id
    origin_access_control_id = aws_cloudfront_origin_access_control.oac.id
    origin_id                = local.s3_origin_id
  }

  enabled             = true
  is_ipv6_enabled     = true

  logging_config {
    include_cookies = false
    bucket          = "transfermarkt-datasets-audit.s3.amazonaws.com"
    prefix          = "cloudfront"
  }

  default_cache_behavior {
    allowed_methods  = ["GET", "HEAD"]
    cached_methods   = ["GET", "HEAD"]
    target_origin_id = local.s3_origin_id

    forwarded_values {
      query_string = false

      cookies {
        forward = "none"
      }
    }

    viewer_protocol_policy = "allow-all"
    min_ttl                = 0
    default_ttl            = 3600
    max_ttl                = 86400
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
      locations = []
    }
  }

  tags = var.tags

}

output "arn" {
  value = aws_cloudfront_distribution.s3_distribution.arn
}
