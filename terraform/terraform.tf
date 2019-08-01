terraform {
  backend "s3" {
    role_arn = "arn:aws:iam::975596993436:role/storage-developer"

    bucket         = "wellcomecollection-storage-infra"
    key            = "terraform/storage.tfstate"
    dynamodb_table = "terraform-locktable"
    region         = "eu-west-1"
  }
}

provider "aws" {
  assume_role {
    role_arn = "arn:aws:iam::975596993436:role/storage-developer"
  }

  region  = "${var.aws_region}"
  version = "1.60.0"
}

provider "aws" {
  alias = "platform"

  assume_role {
    role_arn = "arn:aws:iam::760097843905:role/platform-developer"
  }

  region  = "${var.aws_region}"
  version = "1.60.0"
}

data "terraform_remote_state" "infra_shared" {
  backend = "s3"

  config {
    role_arn = "arn:aws:iam::760097843905:role/platform-developer"

    bucket = "wellcomecollection-platform-infra"
    key    = "terraform/shared_infra.tfstate"
    region = "eu-west-1"
  }
}

data "terraform_remote_state" "infra_critical" {
  backend = "s3"

  config {
    role_arn = "arn:aws:iam::760097843905:role/platform-developer"

    bucket = "wellcomecollection-platform-infra"
    key    = "terraform/catalogue_pipeline_data.tfstate"
    region = "eu-west-1"
  }
}

data "terraform_remote_state" "digitisation_private" {
  backend = "s3"

  config {
    role_arn = "arn:aws:iam::404315009621:role/digitisation-developer"

    bucket = "wellcomedigitisation-infra"
    key    = "terraform/digitisation-private.tfstate"
    region = "eu-west-1"
  }
}

data "terraform_remote_state" "archivematica_infra" {
  backend = "s3"

  config {
    role_arn = "arn:aws:iam::299497370133:role/workflow-developer"

    bucket = "wellcomecollection-workflow-infra"
    key    = "terraform/state/archivematica-infra.tfstate"
    region = "eu-west-1"
  }
}

data "aws_caller_identity" "current" {}
