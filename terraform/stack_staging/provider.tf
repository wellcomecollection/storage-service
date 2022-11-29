locals {
  default_tags = {
    TerraformConfigurationURL = "https://github.com/wellcomecollection/storage-service/tree/main/terraform/stack_staging"
    Department                = "Digital Platform"
    Division                  = "Culture and Society"
    Use                       = "Storage service"
    Environment               = "Staging"
  }
}

provider "aws" {
  assume_role {
    role_arn = "arn:aws:iam::975596993436:role/storage-admin"
  }

  region = var.aws_region

  default_tags {
    tags = local.default_tags
  }
}

provider "aws" {
  alias = "dns"

  assume_role {
    role_arn = "arn:aws:iam::267269328833:role/wellcomecollection-assume_role_hosted_zone_update"
  }

  region = var.aws_region

  default_tags {
    tags = local.default_tags
  }
}

