terraform {
  backend "s3" {
    role_arn     = "arn:aws:iam::975596993436:role/developer"

    bucket         = "wellcomecollection-storage-infra"
    key            = "terraform/storage.tfstate"
    dynamodb_table = "terraform-locktable"
    region         = "eu-west-1"
  }
}

provider "aws" {
  assume_role {
    role_arn     = "arn:aws:iam::975596993436:role/developer"
  }

  region  = "${var.aws_region}"
  version = "1.42.0"
}

data "aws_caller_identity" "current" {}