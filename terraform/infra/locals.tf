locals {
  vpc_id = data.terraform_remote_state.infra_shared.outputs.storage_vpc_id
}

