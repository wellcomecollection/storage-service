provider "aws" {
  assume_role {
    # TODO: Does this need to be the -admin role?  Could it be -developer?
    role_arn = "arn:aws:iam::975596993436:role/storage-admin"
  }

  region  = var.aws_region
  version = "~> 2.0"
}
