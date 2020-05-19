provider "aws" {
  assume_role {
    role_arn = "arn:aws:iam::975596993436:role/storage-developer"
  }

  region  = "eu-west-1"
  version = "~> 2.7"
}
