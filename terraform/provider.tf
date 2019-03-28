provider "aws" {
  assume_role {
    role_arn = "arn:aws:iam::975596993436:role/admin"
  }

  region  = "${var.aws_region}"
  version = "1.60.0"
}

provider "aws" {
  alias = "platform"

  assume_role {
    role_arn = "arn:aws:iam::760097843905:role/developer"
  }

  region  = "${var.aws_region}"
  version = "1.60.0"
}
