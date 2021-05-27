locals {
  default_tags = {
    TerraformConfigurationURL = "https://github.com/wellcomecollection/storage-service/tree/main/terraform/monitoring"
  }
}

provider "aws" {
  assume_role {
    role_arn = "arn:aws:iam::975596993436:role/storage-developer"
  }

  default_tags {
    tags = local.default_tags
  }

  region = "eu-west-1"
}

provider "aws" {
  region = "eu-west-1"

  alias = "platform"

  default_tags {
    tags = local.default_tags
  }

  assume_role {
    role_arn = "arn:aws:iam::760097843905:role/platform-admin"
  }
}
