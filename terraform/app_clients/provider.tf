provider "aws" {
  assume_role {
    role_arn = "arn:aws:iam::760097843905:role/platform-developer"
  }

  region  = "eu-west-1"
  version = "~> 2.7"
}

provider "aws" {
  alias = "storage"

  assume_role {
    role_arn = "arn:aws:iam::975596993436:role/storage-developer"
  }

  region  = "eu-west-1"
  version = "~> 2.7"
}
