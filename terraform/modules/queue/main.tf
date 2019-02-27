data "aws_caller_identity" "current" {}

module "queue" {
  source      = "git::https://github.com/wellcometrust/terraform-modules.git//sqs?ref=v11.6.0"
  queue_name  = "${replace(var.name,"-","_")}"
  aws_region  = "${var.aws_region}"
  account_id  = "${data.aws_caller_identity.current.account_id}"
  topic_names = ["${var.topic_names}"]

  visibility_timeout_seconds = "${var.visibility_timeout_seconds}"
  max_receive_count          = "${var.max_receive_count}"

  alarm_topic_arn = "${var.dlq_alarm_arn}"
}

resource "aws_iam_role_policy" "read_from_q" {
  count = "${length(var.role_names)}"

  role   = "${var.role_names[count.index]}"
  policy = "${module.queue.read_policy}"
}
