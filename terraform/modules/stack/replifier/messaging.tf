# bag_replicator

module "bag_replicator_input_queue" {
  source = "../../queue"

  name = "${var.namespace}_bag_replicator_${var.replica_id}_input"

  topic_arns = var.topic_arns

  role_names = [module.bag_replicator.task_role_name]

  visibility_timeout_seconds = var.replicator_visibility_timeout_seconds

  # We want to make sure the bag replicator doesn't get interrupted mid-work,
  # so we increase the cooldown period to avoid premature scaling down.
  cooldown_period = "15m"

  queue_high_actions = [
    module.bag_replicator.scale_up_arn,
  ]

  queue_low_actions = [
    module.bag_replicator.scale_down_arn,
  ]

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

  visibility_timeout_seconds = var.verifier_visibility_timeout_seconds

  # We want to make sure the bag verifier doesn't get interrupted mid-work,
  # so we increase the cooldown period to avoid premature scaling down.
  cooldown_period = "15m"

  queue_high_actions = [
    module.bag_verifier.scale_up_arn,
  ]

  queue_low_actions = [
    module.bag_verifier.scale_down_arn,
  ]

  dlq_alarm_arn = var.dlq_alarm_arn
}

module "bag_verifier_output_topic" {
  source = "../../topic"

  name = "${var.namespace}_bag_verifier_${var.replica_id}_output"

  role_names = [
    module.bag_verifier.task_role_name,
  ]
}

