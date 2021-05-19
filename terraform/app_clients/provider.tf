locals {
  default_tags = {
    TerraformConfigurationURL = "https://github.com/wellcomecollection/storage-service/tree/main/terraform/app_clients"
  }
}

provider "aws" {
  assume_role {
    role_arn = "arn:aws:iam::760097843905:role/platform-developer"
  }

  region = "eu-west-1"
}

provider "aws" {
  alias = "us-east-1"

  region = "us-east-1"

  assume_role {
    role_arn = "arn:aws:iam::760097843905:role/platform-developer"
  }
}

provider "aws" {
  alias = "platform"

  assume_role {
    role_arn = "arn:aws:iam::760097843905:role/platform-developer"
  }

  region = "eu-west-1"
}

provider "aws" {
  alias = "storage"

  assume_role {
    role_arn = "arn:aws:iam::975596993436:role/storage-developer"
  }

  region = "eu-west-1"
}
