
provider "aws" {
  region = "us-west-2"
  shared_credentials_files = [var.credentials]
  profile = var.profile
  
  endpoints {
    dynamodb = "http://localhost:8000"
  }
}

resource "aws_dynamodb_table" "hydrocron-swot-reach-table" {
    name = "hydrocron-swot-reach-table"
    read_capacity= "30"
    write_capacity= "30"
    hash_key = "reach_id"
    range_key = "range_start_time"
    attribute {
        name = "reach_id"
        type = "S"
    }
    attribute {
        name = "range_start_time"
        type = "S"
    }
}

resource "aws_dynamodb_table" "hydrocron-swot-node-table" {
    name = "hydrocron-swot-node-table"
    read_capacity= "30"
    write_capacity= "30"
    hash_key = "node_id"
    range_key = "range_start_time"
    attribute {
        name = "node_id"
        type = "S"
    }
    attribute {
        name = "range_start_time"
        type = "S"
    }
}