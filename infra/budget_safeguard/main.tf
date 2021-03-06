resource "aws_sns_topic" "budget_safeguard_sns" {
  name = var.sns_name
  tags = var.tags
}

