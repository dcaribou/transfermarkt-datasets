variable "name" {
    type = string
    description = "(optional) describe your variable"
}

variable "execution_role_arn" {
  type = string
}

variable "service_role_arn" {
  type = string
}

resource "aws_batch_compute_environment" "sample" {
  compute_environment_name = "${var.name}-batch-compute"

  compute_resources {
    max_vcpus = 4

    security_group_ids = [
      "sg-e5d26f94" # default sg
    ]

    subnets = [
      "subnet-b48fead2",
      "subnet-e3fc5ab9",
      "subnet-7a730732",
    ]

    type = "FARGATE"
  }

  service_role = var.service_role_arn
  type         = "MANAGED"
}

resource "aws_batch_job_queue" "test_queue" {
  name     = "${var.name}-batch-compute-job-queue"
  state    = "ENABLED"
  priority = 1
  compute_environments = [
    aws_batch_compute_environment.sample.arn
  ]
}

resource "aws_batch_job_definition" "test" {
  name = "${var.name}-batch-job-definition"
  type = "container"
  platform_capabilities = [
    "FARGATE",
  ]

  container_properties = <<CONTAINER_PROPERTIES
{
  "command": ["bash", "prepare_on_batch.sh"],
  "image": "dcaribou/transfermarkt-datasets:fargate-tasks",
  "fargatePlatformConfiguration": {
    "platformVersion": "LATEST"
  },
  "resourceRequirements": [
    {"type": "VCPU", "value": "4"},
    {"type": "MEMORY", "value": "11264"}
  ],
  "executionRoleArn": "${var.execution_role_arn}",
  "jobRoleArn": "${var.execution_role_arn}",
  "networkConfiguration": { 
    "assignPublicIp": "ENABLED"
  }
}
CONTAINER_PROPERTIES
}
