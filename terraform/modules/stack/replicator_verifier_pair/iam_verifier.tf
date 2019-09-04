data "aws_iam_policy_document" "bucket_read" {
  statement {
    actions = [
      "s3:ListBucket",
      "s3:GetObject*",
    ]

    resources = [
      "arn:aws:s3:::${var.bucket_name}",
      "arn:aws:s3:::${var.bucket_name}/*",
    ]
  }
}

resource "aws_iam_role_policy" "bag_verifier_read" {
  role   = "${module.bag_verifier.task_role_name}"
  policy = "${data.aws_iam_policy_document.bucket_read.json}"
}

# The fetch.txt entry may refer to locations in the primary bucket, so we need
# to give verifier read permissions on that bucket as well.

data "aws_iam_policy_document" "primary_bucket_read" {
  statement {
    actions = [
      "s3:GetObject*",
    ]

    resources = [
      "arn:aws:s3:::${var.primary_bucket_name}",
      "arn:aws:s3:::${var.primary_bucket_name}/*",
    ]
  }
}

resource "aws_iam_role_policy" "bag_verifier_primary_read" {
  role   = "${module.bag_verifier.task_role_name}"
  policy = "${data.aws_iam_policy_document.primary_bucket_read.json}"
}

resource "aws_iam_role_policy" "bag_verifier_metrics" {
  role   = "${module.bag_verifier.task_role_name}"
  policy = "${var.cloudwatch_metrics_policy_json}"
}