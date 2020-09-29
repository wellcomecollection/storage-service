terraform {
  backend "s3" {
    role_arn = "arn:aws:iam::975596993436:role/storage-developer"

    bucket         = "wellcomecollection-storage-infra"
    key            = "terraform/storage-service/infra.tfstate"
    dynamodb_table = "terraform-locktable"
    region         = "eu-west-1"
  }
}

data "terraform_remote_state" "accounts_storage" {
  backend = "s3"

  config = {
    role_arn = "arn:aws:iam::760097843905:role/platform-read_only"
    bucket   = "wellcomecollection-platform-infra"
    key      = "terraform/platform-infrastructure/accounts/storage.tfstate"
    region   = "eu-west-1"
  }
}

locals {
  storage_vpcs = data.terraform_remote_state.accounts_storage.outputs
}
