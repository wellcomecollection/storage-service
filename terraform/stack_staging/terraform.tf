terraform {
  backend "s3" {
    role_arn = "arn:aws:iam::975596993436:role/storage-developer"

    bucket         = "wellcomecollection-storage-infra"
    key            = "terraform/storage-service/stack_staging.tfstate"
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

data "terraform_remote_state" "infra_shared" {
  backend = "s3"

  config = {
    role_arn = "arn:aws:iam::760097843905:role/platform-read_only"
    bucket   = "wellcomecollection-platform-infra"
    key      = "terraform/platform-infrastructure/shared.tfstate"
    region   = "eu-west-1"
  }
}

data "terraform_remote_state" "infra_critical" {
  backend = "s3"

  config = {
    role_arn = "arn:aws:iam::760097843905:role/platform-read_only"
    bucket   = "wellcomecollection-platform-infra"
    key      = "terraform/catalogue_pipeline_data.tfstate"
    region   = "eu-west-1"
  }
}

data "terraform_remote_state" "archivematica_infra" {
  backend = "s3"

  config = {
    role_arn = "arn:aws:iam::299497370133:role/workflow-read_only"
    bucket   = "wellcomecollection-workflow-infra"
    key      = "terraform/archivematica-infra/critical_staging.tfstate"
    region   = "eu-west-1"
  }
}

data "terraform_remote_state" "critical_staging" {
  backend = "s3"

  config = {
    role_arn = "arn:aws:iam::975596993436:role/storage-read_only"
    bucket   = "wellcomecollection-storage-infra"
    key      = "terraform/storage-service/critical_staging.tfstate"
    region   = "eu-west-1"
  }
}

