# bag_replicator

module "bag_replicator_input_queue" {
  source = "../../queue"

  name = "${var.namespace}_bag_replicator_${var.replica_id}_input"

  topic_arns = var.topic_arns

  role_names = [module.bag_replicator.task_role_name]

  # Because these operations take a long time (potentially copying thousands
  # of S3 objects for a single message), we keep a high visibility timeout to
  # avoid messages appearing to time out and fail.
  visibility_timeout_seconds = 60 * 60 * 5

  queue_high_actions = [
    module.bag_replicator.scale_up_arn,
  ]

  queue_low_actions = [
    module.bag_replicator.scale_down_arn,
  ]

  aws_region    = var.aws_region
  dlq_alarm_arn = var.dlq_alarm_arn
}

module "bag_replicator_output_topic" {
  source = "../../topic"

  name = "${var.namespace}_bag_replicator_${var.replica_id}_output"

  role_names = [
    module.bag_replicator.task_role_name,
  ]
}

# bag_verifier

module "bag_verifier_queue" {
  source = "../../queue"

  name = "${var.namespace}_bag_verifier_${var.replica_id}_input"

  topic_arns = [module.bag_replicator_output_topic.arn]

  role_names = [module.bag_verifier.task_role_name]

  # We keep a high visibility timeout to
  # avoid messages appearing to time out and fail.
  visibility_timeout_seconds = 60 * 60 * 5

  queue_high_actions = [
    module.bag_verifier.scale_up_arn,
  ]

  queue_low_actions = [
    module.bag_verifier.scale_down_arn,
  ]

  aws_region    = var.aws_region
  dlq_alarm_arn = var.dlq_alarm_arn
}

module "bag_verifier_output_topic" {
  source = "../../topic"

  name = "${var.namespace}_bag_verifier_${var.replica_id}_output"

  role_names = [
    module.bag_verifier.task_role_name,
  ]
}

