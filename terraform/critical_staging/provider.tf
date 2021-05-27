locals {
  default_tags = {
    TerraformConfigurationURL = "https://github.com/wellcomecollection/storage-service/tree/main/terraform/critical_staging"
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

provider "azurerm" {
  features {}
}
