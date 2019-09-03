locals {
  key_name = "wellcomedigitalstorage"

  vpc_id         = "${data.terraform_remote_state.infra_shared.storage_vpc_id}"
  public_subnets = "${data.terraform_remote_state.infra_shared.storage_vpc_public_subnets}"

  archivematica_ingests_bucket = "${data.terraform_remote_state.archivematica_infra.ingests_bucket}"
}
