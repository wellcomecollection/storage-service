locals {
  default_tags = {
    TerraformConfigurationURL = "https://github.com/wellcomecollection/storage-service/tree/main/terraform/infra"
  }
}


provider "aws" {
  assume_role {
    role_arn = "arn:aws:iam::975596993436:role/storage-admin"
  }

  default_tags {
    tags = local.default_tags
  }

  region = var.aws_region
}

provider "aws" {
  region = "us-east-1"
  alias  = "us_east_1"

  assume_role {
    role_arn = "arn:aws:iam::975596993436:role/storage-developer"
  }

  default_tags {
    tags = local.default_tags
  }
}
