data "aws_s3_bucket" "static_content_bucket" {
  bucket = "${aws_s3_bucket_object.context.bucket}"
}

data "aws_iam_policy_document" "archive_static_content_get" {
  statement {
    actions = [
      "s3:GetObject*",
    ]

    resources = [
      "${data.aws_s3_bucket.static_content_bucket.arn}/${aws_s3_bucket_object.context.key}",
    ]
  }
}

data "aws_iam_policy_document" "allow_ingests_publish_to_unpacker_topic" {
  statement {
    actions = [
      "sns:Publish",
    ]

    resources = [
      "${var.bag_unpacker_topic_arn}",
    ]
  }
}