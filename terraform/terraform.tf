# Provider

provider "aws" {
  region  = "${var.aws_region}"
  version = "1.42.0"
}

# Terraform

terraform {
  required_version = ">= 0.9"

  backend "s3" {
    bucket         = "wellcomecollection-storage-infra"
    key            = "terraform/storage.tfstate"
    dynamodb_table = "terraform-locktable"
    region         = "eu-west-1"
  }
}