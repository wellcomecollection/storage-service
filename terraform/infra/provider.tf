provider "aws" {
  assume_role {
    role_arn = "arn:aws:iam::975596993436:role/storage-developer"
  }

  region  = var.aws_region
  version = "2.34.0"
}

