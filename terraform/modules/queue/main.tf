data "aws_caller_identity" "current" {}

locals {
  queue_name = "${replace(var.name,"-","_")}"
}

module "queue" {
  source      = "git::https://github.com/wellcometrust/terraform-modules.git//sqs?ref=58303f9"
  queue_name  = "${replace(var.name,"-","_")}"
  aws_region  = "${var.aws_region}"
  account_id  = "${data.aws_caller_identity.current.account_id}"
  topic_names = ["${var.topic_names}"]
  topic_count = "${length(var.topic_names)}"

  visibility_timeout_seconds = "${var.visibility_timeout_seconds}"
  max_receive_count          = "${var.max_receive_count}"

  alarm_topic_arn = "${var.dlq_alarm_arn}"
}

module "scaling_alarm" {
  source     = "git::https://github.com/wellcometrust/terraform-modules.git//autoscaling/alarms/queue?ref=v19.10.0"
  queue_name = "${local.queue_name}"

  queue_high_actions = ["${var.queue_high_actions}"]
  queue_low_actions  = ["${var.queue_low_actions}"]
}

resource "aws_iam_role_policy" "read_from_q" {
  count = "${length(var.role_names)}"

  role   = "${var.role_names[count.index]}"
  policy = "${module.queue.read_policy}"
}

resource "aws_cloudwatch_metric_alarm" "queue_high" {
  alarm_name          = "${local.queue_name}_high"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  threshold           = "1"
  alarm_description   = "Queue high"

  alarm_actions = ["${var.queue_high_actions}"]

  namespace   = "AWS/SQS"
  metric_name = "ApproximateNumberOfMessagesVisible"
  period      = "60"

  statistic = "Sum"

  dimensions = {
    QueueName = "${local.queue_name}"
  }

  insufficient_data_actions = []
}
