terraform {
  backend "s3" {
    role_arn = "arn:aws:iam::975596993436:role/storage-developer"

    bucket         = "wellcomecollection-storage-infra"
    key            = "terraform/storage-service/monitoring.tfstate"
    dynamodb_table = "terraform-locktable"
    region         = "eu-west-1"
  }
}

data "terraform_remote_state" "stack_staging" {
  backend = "s3"

  config = {
    role_arn = "arn:aws:iam::975596993436:role/storage-read_only"
    bucket   = "wellcomecollection-storage-infra"
    key      = "terraform/storage-service/stack_staging.tfstate"
    region   = "eu-west-1"
  }
}
