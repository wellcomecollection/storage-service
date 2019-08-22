data "aws_iam_policy_document" "bucket_readwrite" {
  statement {
    actions = [
      "s3:PutObject*",
      "s3:GetObject*",
    ]

    resources = [
      "arn:aws:s3:::${var.bucket_name}/*",
    ]
  }
}

resource "aws_iam_role_policy" "bag_replicator_read_write" {
  role   = "${module.bag_replicator.task_role_name}"
  policy = "${data.aws_iam_policy_document.bucket_readwrite.json}"
}

resource "aws_iam_role_policy" "bag_replicator_read_ingests_s3" {
  role   = "${module.bag_replicator.task_role_name}"
  policy = "${var.ingests_read_policy_json}"
}

resource "aws_iam_role_policy" "bag_replicator_metrics" {
  role   = "${module.bag_replicator.task_role_name}"
  policy = "${var.cloudwatch_metrics_policy_json}"
}

resource "aws_iam_role_policy" "bag_replicator_lock_table" {
  role   = "${module.bag_replicator.task_role_name}"
  policy = "${var.replicator_lock_table_policy_json}"
}
