provider "aws" {
  assume_role {
    role_arn = "arn:aws:iam::975596993436:role/storage-admin"
  }

  region = var.aws_region

  # Ignore deployment tags on services
  ignore_tags {
    keys = ["deployment:label"]
  }
}

