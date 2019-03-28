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

data "aws_iam_policy_document" "archivematica_ingests_bucket_policy" {
  statement {
    actions = [
      "s3:Get*",
    ]

    resources = [
      "arn:aws:s3:::${local.archivematica_ingests_bucket}/*",
    ]

    principals {
      type = "AWS"

      identifiers = [
        "${data.aws_iam_role.colbert_bag_unpacker_task_role.arn}",
        "${data.aws_iam_role.stewart_bag_unpacker_task_role.arn}",
      ]
    }
  }
}

resource "aws_s3_bucket_policy" "archivematica_ingests_bucket_policy" {
  bucket = "${local.archivematica_ingests_bucket}"
  policy = "${data.aws_iam_policy_document.archivematica_ingests_bucket_policy.json}"

  provider = "aws.workflow"
}
