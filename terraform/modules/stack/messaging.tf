# ingests

module "ingests_topic" {
  source = "../topic"

  name = "${var.namespace}_ingests"

  role_names = [
    module.bag_register.task_role_name,
    module.bag_root_finder.task_role_name,
    module.bag_verifier_pre_replication.task_role_name,
    module.bag_unpacker.task_role_name,
    module.ingests.task_role_name,
    module.notifier.task_role_name,
    module.bag_versioner.task_role_name,
    module.replica_aggregator.task_role_name,
    module.replicator_verifier_primary.replicator_task_role_name,
    module.replicator_verifier_primary.verifier_task_role_name,
    module.replicator_verifier_glacier.replicator_task_role_name,
    module.replicator_verifier_glacier.verifier_task_role_name,
  ]
}

module "ingests_input_queue" {
  source = "../queue"

  name = "${var.namespace}_ingests_input"

  topic_arns = [module.ingests_topic.arn]

  role_names = [
    module.ingests.task_role_name,
  ]

  queue_high_actions = [
    module.ingests.scale_up_arn,
  ]

  queue_low_actions = [
    module.ingests.scale_down_arn,
  ]

  aws_region    = var.aws_region
  dlq_alarm_arn = var.dlq_alarm_arn

  # Updates sent to the ingests monitor can fail with a ConditionalUpdate error
  # if multiple updates arrive at the same time, and eventually land on the DLQ.
  #
  # We should fix this properly, but for now we just retry this queue more
  # times until they eventually go through.
  max_receive_count = 10
}

module "updated_ingests_topic" {
  source = "../topic"

  name       = "${var.namespace}_updated_ingests"
  role_names = [module.ingests.task_role_name]
}

module "ingests_monitor_callback_notifications_topic" {
  source = "../topic"

  name       = "${var.namespace}_ingests_monitor_callback_notifications"
  role_names = [module.ingests.task_role_name]
}

# notifier

module "notifier_input_queue" {
  source = "../queue"

  name = "${var.namespace}_notifier"

  topic_arns = [module.ingests_monitor_callback_notifications_topic.arn]

  role_names = [module.notifier.task_role_name]

  queue_high_actions = [
    module.notifier.scale_up_arn,
  ]

  queue_low_actions = [
    module.notifier.scale_down_arn,
  ]

  aws_region    = var.aws_region
  dlq_alarm_arn = var.dlq_alarm_arn
}

# bag_unpacker

module "bag_unpacker_input_topic" {
  source = "../topic"

  name = "${var.namespace}_bag_unpacker_input"

  role_names = [
    module.bag_unpacker.task_role_name,
  ]
}

module "bag_unpacker_queue" {
  source = "../queue"

  name = "${var.namespace}_bag_unpacker_input"

  topic_arns = [module.bag_unpacker_input_topic.arn]

  role_names = [module.bag_unpacker.task_role_name]

  # We keep a high visibility timeout to
  # avoid messages appearing to time out and fail.
  visibility_timeout_seconds = 60 * 60 * 5

  queue_high_actions = [
    module.bag_unpacker.scale_up_arn,
  ]

  queue_low_actions = [
    module.bag_unpacker.scale_down_arn,
  ]

  aws_region    = var.aws_region
  dlq_alarm_arn = var.dlq_alarm_arn
}

module "bag_unpacker_output_topic" {
  source = "../topic"

  name = "${var.namespace}_bag_unpacker_output"

  role_names = [
    module.bag_unpacker.task_role_name,
  ]
}

# bag root finder

module "bag_root_finder_queue" {
  source = "../queue"

  name = "${var.namespace}_bag_root_finder_input"

  topic_arns = [module.bag_unpacker_output_topic.arn]

  role_names = [module.bag_root_finder.task_role_name]

  queue_high_actions = [
    module.bag_root_finder.scale_up_arn,
  ]

  queue_low_actions = [
    module.bag_root_finder.scale_down_arn,
  ]

  aws_region    = var.aws_region
  dlq_alarm_arn = var.dlq_alarm_arn
}

module "bag_root_finder_output_topic" {
  source = "../topic"

  name = "${var.namespace}_bag_root_finder_output"

  role_names = [
    module.bag_root_finder.task_role_name,
  ]
}

# bag_verifier pre-replication

module "bag_verifier_pre_replicate_queue" {
  source = "../queue"

  name = "${var.namespace}_bag_verifier_pre_replicate_input"

  topic_arns = [module.bag_root_finder_output_topic.arn]

  role_names = [module.bag_verifier_pre_replication.task_role_name]

  # We keep a high visibility timeout to
  # avoid messages appearing to time out and fail.
  visibility_timeout_seconds = 60 * 60 * 5

  queue_high_actions = [
    module.bag_verifier_pre_replication.scale_up_arn,
  ]

  queue_low_actions = [
    module.bag_verifier_pre_replication.scale_down_arn,
  ]

  aws_region    = var.aws_region
  dlq_alarm_arn = var.dlq_alarm_arn
}

module "bag_verifier_pre_replicate_output_topic" {
  source = "../topic"

  name = "${var.namespace}_bag_verifier_pre_replicate_output"

  role_names = [
    module.bag_verifier_pre_replication.task_role_name,
  ]
}

# bag versioner

module "bag_versioner_queue" {
  source = "../queue"

  name = "${var.namespace}_bag_versioner_input"

  topic_arns = [module.bag_verifier_pre_replicate_output_topic.arn]

  role_names = [module.bag_versioner.task_role_name]

  queue_high_actions = [
    module.bag_versioner.scale_up_arn,
  ]

  queue_low_actions = [
    module.bag_versioner.scale_down_arn,
  ]

  aws_region    = var.aws_region
  dlq_alarm_arn = var.dlq_alarm_arn
}

module "bag_versioner_output_topic" {
  source = "../topic"

  name = "${var.namespace}_bag_versioner_output"

  role_names = [
    module.bag_versioner.task_role_name,
  ]
}

# replica_aggregator

module "replica_aggregator_input_queue" {
  source = "../queue"

  name = "${var.namespace}_replica_aggregator_input"

  topic_arns = [
    module.replicator_verifier_primary.verifier_output_topic_arn,
    module.replicator_verifier_glacier.verifier_output_topic_arn,
  ]

  role_names = [module.replica_aggregator.task_role_name]

  queue_high_actions = [
    module.replica_aggregator.scale_up_arn,
  ]

  queue_low_actions = [
    module.replica_aggregator.scale_down_arn,
  ]

  aws_region    = var.aws_region
  dlq_alarm_arn = var.dlq_alarm_arn

  # The aggregator may have to retry messages if two replicas complete
  # at the same time, so we need to be able to receive messages more than once.
  max_receive_count = 3
}

module "replica_aggregator_output_topic" {
  source = "../topic"

  name = "${var.namespace}_replica_aggregator_output"

  role_names = [
    module.replica_aggregator.task_role_name,
  ]
}

# bag_register

module "bag_register_input_queue" {
  source = "../queue"

  name = "${var.namespace}_bag_register_input"

  topic_arns = [module.replica_aggregator_output_topic.arn]

  role_names = [module.bag_register.task_role_name]

  queue_high_actions = [
    module.bag_register.scale_up_arn,
  ]

  queue_low_actions = [
    module.bag_register.scale_down_arn,
  ]

  aws_region    = var.aws_region
  dlq_alarm_arn = var.dlq_alarm_arn

  # Only a handful of big bags take more than half an hour to assemble
  # the storage manifest, and currently the bag register doesn't handle getting
  # duplicate messages well.
  #
  # Setting the timeout this high means we'll get fast retries if somebody goes
  # wrong, but we won't get the register trying to process the same ingest twice
  # if it's moderately-sized.
  #
  # See https://github.com/wellcometrust/platform/issues/3889
  visibility_timeout_seconds = 1800
}

module "bag_register_output_topic" {
  source = "../topic"

  name = "${var.namespace}_bag_register_output"

  role_names = [
    module.bag_register.task_role_name,
  ]
}

resource "aws_sns_topic_policy" "bag_register_output_topic_cross_account_subscription" {
  # We only need to create a policy that allows subscriptions to this topic
  # if there are other accounts that need access.
  count = length(var.bag_register_output_subscribe_principals) > 0 ? 1 : 0

  arn    = module.bag_register_output_topic.arn
  policy = data.aws_iam_policy_document.bag_register_output_cross_account_subscription.json
}

module "bag_register_output_queue" {
  source = "../queue"

  name = "${var.namespace}_bag_register_output"

  topic_arns = [module.bag_register_output_topic.arn]

  role_names = []

  aws_region    = var.aws_region
  dlq_alarm_arn = var.dlq_alarm_arn
}

