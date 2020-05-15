terraform {
  backend "s3" {
    role_arn = "arn:aws:iam::975596993436:role/storage-developer"

    bucket         = "wellcomecollection-storage-infra"
    key            = "terraform/storage-service/app_clients.tfstate"
    dynamodb_table = "terraform-locktable"
    region         = "eu-west-1"
  }
}
