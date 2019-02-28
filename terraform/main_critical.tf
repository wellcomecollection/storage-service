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

  service-wt-winnipeg = "${data.terraform_remote_state.infra_shared.service-wt-winnipeg}"
  service-pl-winslow  = "${data.terraform_remote_state.infra_shared.service-pl-winslow}"

  archive_read_principles = [
    "${local.goobi_task_role_arn}",
    "${aws_iam_user.dds_digirati.arn}",
  ]

  access_read_principles = [
    "${local.goobi_task_role_arn}",
    "${aws_iam_user.dds_digirati.arn}",
    "arn:aws:iam::653428163053:user/echo-fs",
    "arn:aws:iam::653428163053:user/api",
  ]
}

module "critical-staging" {
  source = "critical"

  namespace  = "${local.namespace}-staging"
  account_id = "${local.account_id}"

  service-wt-winnipeg = "${data.terraform_remote_state.infra_shared.service-wt-winnipeg}"
  service-pl-winslow  = "${data.terraform_remote_state.infra_shared.service-pl-winslow}"

  archive_read_principles = [
    "${local.goobi_task_role_arn}",
    "${aws_iam_user.dds_digirati.arn}",
  ]

  access_read_principles = [
    "${local.goobi_task_role_arn}",
    "${aws_iam_user.dds_digirati.arn}",
    "arn:aws:iam::653428163053:user/echo-fs",
    "arn:aws:iam::653428163053:user/api",
  ]
}

module "bastion" {
  source = "git::https://github.com/wellcometrust/terraform.git//ec2/prebuilt/bastion?ref=v17.1.0"

  vpc_id = "${local.vpc_id}"

  name = "storage-bastion"

  controlled_access_cidr_ingress = ["${local.admin_cidr_ingress}"]

  key_name    = "${local.key_name}"
  subnet_list = "${local.public_subnets}"
}
