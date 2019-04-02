resource "aws_iam_user" "dds_digirati" {
  name = "dds_digirati"
}

resource "aws_iam_access_key" "dds_digirati" {
  user    = "${aws_iam_user.dds_digirati.name}"
  pgp_key = "keybase:kenoir"
}

data "aws_iam_role" "colbert_bag_unpacker_task_role" {
  name = "${module.stack-colbert.unpacker_task_role_name}"
}

data "aws_iam_role" "stewart_bag_unpacker_task_role" {
  name = "${module.stack-stewart.unpacker_task_role_name}"
}
