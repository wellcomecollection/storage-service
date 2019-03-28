data "terraform_remote_state" "archivematica_infra" {
  backend = "s3"

  config {
    role_arn = "arn:aws:iam::299497370133:role/developer"

    bucket = "wellcomecollection-workflow-infra"
    key    = "terraform/state/archivematica-infra.tfstate"
    region = "eu-west-1"
  }
}

locals {
  archivematica_ingests_bucket = "${data.terraform_remote_state.archivematica_infra.ingests_bucket}"
}
