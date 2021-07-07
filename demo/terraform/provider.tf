locals {
  namespace       = "weco-dams-prototype"
  short_namespace = "weco"

  aws_region = data.aws_region.current.name
  account_id = "241906670800"

  cidr_block = "172.14.0.0/16"
}

data "aws_region" "current" {}

terraform {
  backend "s3" {
    role_arn = "arn:aws:iam::975596993436:role/storage-developer"

    bucket         = "wellcomecollection-storage-infra"
    key            = "terraform/dams-prototype-project/main.tfstate"
    dynamodb_table = "terraform-locktable"
    region         = "eu-west-1"
  }
}

provider "aws" {
  assume_role {
    role_arn = "arn:aws:iam::241906670800:role/dam_prototype-admin"
  }

  region = "eu-west-1"
}