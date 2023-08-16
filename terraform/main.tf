
provider "aws" {
  region = "us-west-2"
  shared_credentials_file = var.credentials
  profile = var.profile

  ignore_tags {
    key_prefixes = ["gsfc-ngap"]
  }
}

locals {
  name        = var.app_name
  environment = var.stage

  account_id = data.aws_caller_identity.current.account_id

  # This is the convention we use to know what belongs to each other
  ec2_resources_name = "service-${local.name}-${local.environment}"

  # Used to refer to the HYDROCRON database resources by the same convention
  hydrocrondb_resource_name = "service-${var.db_app_name}-${local.environment}"

  default_tags = length(var.default_tags) == 0 ? {
    team: "TVA",
    application: local.ec2_resources_name,
    Environment = var.stage
    Version = var.docker_tag
  } : var.default_tags
}

data "aws_caller_identity" "current" {}

resource "aws_dynamodb_table" "hydrocron_swot_reaches_test" {
    name = "hydrocron_swot_reaches_test"
    billing_mode = "PROVISIONED"
    read_capacity= "30"
    write_capacity= "30"
    attribute {
        name = "reach_id"
        type = "S"
    }
    hash_key = "reach_id"
    ttl {
     enabled = true
     attribute_name = "expiryPeriod"
    }
    point_in_time_recovery { enabled = true }
    server_side_encryption { enabled = true }
    lifecycle { ignore_changes = [ write_capacity, read_capacity ] }
}

module  "table_autoscaling" {
   source = "snowplow-devops/dynamodb-autoscaling/aws"
   table_name = aws_dynamodb_table.hydrocron_swot_reaches_test.name
}