variable "bucket_name" {
  type = string
  description = "The name of the base bucket for the project"
}

variable "user_name" {
  type = string
  description = "The name of the user for the project"
}

variable "authorized_users" {
  type = list(string)
  description = "IAM users who are authorized to read dvc/ objects"
  default = []
}

variable "tags" {
  type = map
  description = "Project tags"
}
