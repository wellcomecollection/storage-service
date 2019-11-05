locals {
  vpc_id = "${data.terraform_remote_state.infra_shared.storage_vpc_id}"

  archivematica_ingests_bucket = "${data.terraform_remote_state.archivematica_infra.ingests_bucket}"
}
