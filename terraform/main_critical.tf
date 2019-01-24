resource "aws_s3_bucket" "infra" {
  bucket = "wellcomecollection-storage-infra"
  acl    = "private"

  lifecycle {
    prevent_destroy = true
  }

  versioning {
    enabled = true
  }
}

module "critical" {
  source = "critical"

  namespace  = "${local.namespace}"
  account_id = "${local.account_id}"

  environment_name = "production"

  vpc_id = "${local.vpc_id}"

  private_cidr_block_ids = ["${local.private_subnet_ids}"]

  service-wt-winnipeg = "${data.terraform_remote_state.infra_shared.service-wt-winnipeg}"
  service-pl-winslow  = "${data.terraform_remote_state.infra_shared.service-pl-winslow}"

  subnets_ids = [
    "${data.terraform_remote_state.infra_shared.storage_vpc_private_subnets[0]}",
    "${data.terraform_remote_state.infra_shared.storage_vpc_private_subnets[2]}",
  ]

  archive_readaccess_principles = [
    "${local.goobi_task_role_arn}",
    "${aws_iam_user.dds_digirati.arn}",
  ]
}

module "critical-staging" {
  source = "critical"

  namespace  = "${local.namespace}-staging"
  account_id = "${local.account_id}"

  environment_name = "staging"

  vpc_id = "${local.vpc_id}"

  private_cidr_block_ids = ["${local.private_subnet_ids}"]

  service-wt-winnipeg = "${data.terraform_remote_state.infra_shared.service-wt-winnipeg}"
  service-pl-winslow  = "${data.terraform_remote_state.infra_shared.service-pl-winslow}"

  subnets_ids = [
    "${data.terraform_remote_state.infra_shared.storage_vpc_private_subnets[0]}",
    "${data.terraform_remote_state.infra_shared.storage_vpc_private_subnets[2]}",
  ]

  archive_readaccess_principles = [
    "${local.goobi_task_role_arn}",
    "${aws_iam_user.dds_digirati.arn}",
  ]
}

module "bastion" {
  source = "git::https://github.com/wellcometrust/terraform.git//ec2/prebuilt/bastion?ref=v17.1.0"

  vpc_id = "${local.vpc_id}"

  name = "storage-bastion"

  controlled_access_cidr_ingress = ["${local.admin_cidr_ingress}"]

  key_name    = "${var.key_name}"
  subnet_list = "${local.public_subnets}"
}
