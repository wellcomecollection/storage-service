terraform {
  backend "s3" {
    role_arn = "arn:aws:iam::975596993436:role/storage-developer"

    bucket         = "wellcomecollection-storage-infra"
    key            = "terraform/storage-service/critical_staging.tfstate"
    dynamodb_table = "terraform-locktable"
    region         = "eu-west-1"
  }
}

data "terraform_remote_state" "digitisation_private" {
  backend = "s3"

  config {
    role_arn = "arn:aws:iam::404315009621:role/digitisation-read_only"

    bucket = "wellcomedigitisation-infra"
    key    = "terraform/digitisation-private.tfstate"
    region = "eu-west-1"
  }
}
