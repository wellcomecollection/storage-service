module "queue" {
  source     = "git::github.com/wellcomecollection/terraform-aws-sqs//queue?ref=v1.2.1"
  queue_name = var.name
  topic_arns = var.topic_arns

  visibility_timeout_seconds = var.visibility_timeout_seconds
  max_receive_count          = var.max_receive_count

  alarm_topic_arn = var.dlq_alarm_arn
}

module "scaling_alarm" {
  source     = "git::github.com/wellcomecollection/terraform-aws-sqs//autoscaling?ref=v1.3.0"
  queue_name = module.queue.name

  queue_high_actions = var.queue_high_actions
  queue_low_actions  = var.queue_low_actions

  cooldown_period = var.cooldown_period
}

resource "aws_iam_role_policy" "read_from_q" {
  count = length(var.role_names)

  role   = var.role_names[count.index]
  policy = module.queue.read_policy
}
