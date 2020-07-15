locals {
  default_tags = {
    TerraformConfigurationURL = "https://github.com/wellcomecollection/storage-service/tree/master/terraform/critical_prod"
  }
}

provider "aws" {
  assume_role {
    role_arn = "arn:aws:iam::975596993436:role/storage-admin"
  }

  region  = var.aws_region
  version = "2.34.0"
}

provider "azurerm" {
  version = "=2.0.0"
  features {}
}
