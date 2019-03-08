data "aws_caller_identity" "current" {}

locals {
  queue_name = "${replace(var.name,"-","_")}"
}

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

resource "aws_cloudwatch_metric_alarm" "queue_high" {
  alarm_name          = "${local.queue_name}_high"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  threshold           = "100"
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

resource "aws_cloudwatch_metric_alarm" "queue_low" {
  alarm_name          = "${local.queue_name}_low"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = "1"
  threshold           = "1"
  alarm_description   = "Queue low"

  alarm_actions = ["${var.queue_low_actions}"]

  insufficient_data_actions = []

  metric_query {
    id          = "e1"
    expression  = "m1+m2"
    label       = "ApproximateNumberOfMessagesTotal"
    return_data = "true"
  }

  metric_query {
    id = "m1"

    metric {
      metric_name = "ApproximateNumberOfMessagesVisible"
      namespace   = "AWS/SQS"
      period      = "60"
      stat        = "Sum"

      dimensions = {
        QueueName = "${local.queue_name}"
      }
    }
  }

  metric_query {
    id = "m2"

    metric {
      metric_name = "ApproximateNumberOfMessagesNotVisible"
      namespace   = "AWS/SQS"
      period      = "60"
      stat        = "Sum"

      dimensions = {
        QueueName = "${local.queue_name}"
      }
    }
  }
}
