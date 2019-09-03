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
    role_arn = "arn:aws:iam::975596993436:role/storage-admin"
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
    role_arn = "arn:aws:iam::760097843905:role/platform-read_only"

    bucket = "wellcomecollection-platform-infra"
    key    = "terraform/shared_infra.tfstate"
    region = "eu-west-1"
  }
}

data "terraform_remote_state" "infra_critical" {
  backend = "s3"

  config {
    role_arn = "arn:aws:iam::760097843905:role/platform-read_only"

    bucket = "wellcomecollection-platform-infra"
    key    = "terraform/catalogue_pipeline_data.tfstate"
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

data "terraform_remote_state" "critical_prod" {
  backend = "s3"

  config {
    role_arn = "arn:aws:iam::975596993436:role/storage-read_only"

    bucket = "wellcomecollection-storage-infra"
    key    = "terraform/storage-service/critical_prod.tfstate"
    region = "eu-west-1"
  }
}

data "terraform_remote_state" "critical_staging" {
  backend = "s3"

  config {
    role_arn = "arn:aws:iam::975596993436:role/storage-read_only"

    bucket = "wellcomecollection-storage-infra"
    key    = "terraform/storage-service/critical_staging.tfstate"
    region = "eu-west-1"
  }
}
