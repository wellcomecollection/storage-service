locals {
  default_tags = {
    TerraformConfigurationURL = "https://github.com/wellcomecollection/storage-service/tree/main/terraform/stack_prod"
    Department                = "Digital Platform"
    Division                  = "Culture and Society"
    Use                       = "Storage service"
    Environment               = "Production"
  }
}

provider "aws" {
  assume_role {
    role_arn = "arn:aws:iam::975596993436:role/storage-admin"
  }

  region = "eu-west-1"

  default_tags {
    tags = local.default_tags
  }
}

provider "aws" {
  alias = "dns"

  assume_role {
    role_arn = "arn:aws:iam::267269328833:role/wellcomecollection-assume_role_hosted_zone_update"
  }

  region = "eu-west-1"

  default_tags {
    tags = local.default_tags
  }
}
