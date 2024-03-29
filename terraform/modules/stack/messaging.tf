# ingests

module "ingests_topic" {
  source = "../topic"

  name = "${var.namespace}_ingests"

  role_names = concat(
    [
      module.bag_register.task_role_name,
      module.bag_root_finder.task_role_name,
      module.bag_verifier_pre_replication.task_role_name,
      module.bag_unpacker.task_role_name,
      module.notifier.task_role_name,
      module.bag_versioner.task_role_name,
      module.replica_aggregator.task_role_name,
      module.replicator_verifier_primary.replicator_task_role_name,
      module.replicator_verifier_primary.verifier_task_role_name,
      module.replicator_verifier_glacier.replicator_task_role_name,
      module.replicator_verifier_glacier.verifier_task_role_name,

    ],
    module.replicator_verifier_azure.*.replicator_task_role_name,
    module.replicator_verifier_azure.*.verifier_task_role_name,
  )
}

module "ingests_input_queue" {
  source = "../queue"

  name = "${var.namespace}_ingests_input"

  topic_arns = [module.ingests_topic.arn]

  role_names = [
    module.ingest_service.task_role_name,
  ]

  queue_high_actions = []

  queue_low_actions = []

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
  role_names = [module.ingest_service.task_role_name]
}

module "ingests_monitor_callback_notifications_topic" {
  source = "../topic"

  name       = "${var.namespace}_ingests_monitor_callback_notifications"
  role_names = [module.ingest_service.task_role_name]
}

module "updated_ingests_queue" {
  source = "../queue"

  name = "${var.namespace}_updated_ingests"

  topic_arns = [module.updated_ingests_topic.arn]

  role_names = [
    module.ingests_indexer.task_role_name,
  ]

  queue_high_actions = [
    module.ingests_indexer.scale_up_arn,
  ]

  queue_low_actions = [
    module.ingests_indexer.scale_down_arn,
  ]

  dlq_alarm_arn = var.dlq_alarm_arn
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

  dlq_alarm_arn = var.dlq_alarm_arn
}

# bag_unpacker

module "bag_unpacker_input_topic" {
  source = "../topic"

  name = "${var.namespace}_bag_unpacker_input"

  role_names = [
    module.ingest_service.task_role_name,
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

  # We want to make sure the bag unpacker doesn't get interrupted mid-work,
  # so we increase the cooldown period to avoid premature scaling down.
  cooldown_period = "15m"

  queue_high_actions = [
    module.bag_unpacker.scale_up_arn,
  ]

  queue_low_actions = [
    module.bag_unpacker.scale_down_arn,
  ]

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

  dlq_alarm_arn = var.dlq_alarm_arn

  # We want to make sure the bag verifier doesn't get interrupted mid-work,
  # so we increase the cooldown period to avoid premature scaling down.
  cooldown_period = "15m"
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

  topic_arns = concat(
    [
      module.replicator_verifier_primary.verifier_output_topic_arn,
      module.replicator_verifier_glacier.verifier_output_topic_arn,
    ],
    module.replicator_verifier_azure.*.verifier_output_topic_arn,
  )

  role_names = [module.replica_aggregator.task_role_name]

  queue_high_actions = [
    module.replica_aggregator.scale_up_arn,
  ]

  queue_low_actions = [
    module.replica_aggregator.scale_down_arn,
  ]

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

module "registered_bag_notifications_topic" {
  source = "../topic"

  name = "${var.namespace}_registered_bag_notifications"

  role_names = [
    module.bag_register.task_role_name,
  ]
}

resource "aws_sns_topic_policy" "registered_bag_notifications_topic_cross_account_subscription" {
  # We only need to create a policy that allows subscriptions to this topic
  # if there are other accounts that need access.
  count = length(var.allow_cross_account_subscription_to_bag_register_output_from) > 0 ? 1 : 0

  arn    = module.registered_bag_notifications_topic.arn
  policy = data.aws_iam_policy_document.allow_bag_registration_notification_subscription.json
}

module "registered_bag_notifications_queue" {
  source = "../queue"

  name = "${var.namespace}_registered_bag_notifications"

  topic_arns = [
    module.registered_bag_notifications_topic.arn
  ]

  role_names = []

  dlq_alarm_arn = var.dlq_alarm_arn
}

# bag indexer

module "bag_reindexer_output_topic" {
  source = "../topic"

  name = "${var.namespace}_bag_reindexer_output"

  role_names = []
}

module "bag_indexer_input_queue" {
  source = "../queue"

  name = "${var.namespace}_bag_indexer_input"

  topic_arns = [
    module.registered_bag_notifications_topic.arn,
    module.bag_reindexer_output_topic.arn
  ]

  role_names = [module.bag_indexer.task_role_name]

  queue_high_actions = [
    module.bag_indexer.scale_up_arn,
  ]

  queue_low_actions = [
    module.bag_indexer.scale_down_arn,
  ]

  dlq_alarm_arn = var.dlq_alarm_arn
}

# file finder

module "file_reindexer_output_topic" {
  source = "../topic"

  name = "${var.namespace}_file_reindexer_output"

  role_names = []
}

module "file_finder_input_queue" {
  source = "../queue"

  name = "${var.namespace}_file_finder_input"

  topic_arns = [
    module.registered_bag_notifications_topic.arn,
    module.file_reindexer_output_topic.arn,
  ]

  role_names = [module.file_finder.task_role_name]

  queue_high_actions = [
    module.file_finder.scale_up_arn,
  ]

  queue_low_actions = [
    module.file_finder.scale_down_arn,
  ]

  dlq_alarm_arn = var.dlq_alarm_arn
}

module "file_finder_output_topic" {
  source = "../topic"

  name = "${var.namespace}_file_finder_output"

  role_names = [
    module.file_finder.task_role_name,
  ]
}

# file indexer

module "file_indexer_input_queue" {
  source = "../queue"

  name = "${var.namespace}_file_indexer_input"

  topic_arns = [
    module.file_finder_output_topic.arn,
  ]

  role_names = [module.file_indexer.task_role_name]

  queue_high_actions = [
    module.file_indexer.scale_up_arn,
  ]

  queue_low_actions = [
    module.file_indexer.scale_down_arn,
  ]

  dlq_alarm_arn = var.dlq_alarm_arn
}


# bag tagger

module "bag_tagger_input_queue" {
  source = "../queue"

  name = "${var.namespace}_bag_tagger_input"

  topic_arns = [
    module.registered_bag_notifications_topic.arn,
  ]

  role_names = [module.bag_tagger.task_role_name]

  queue_high_actions = [
    module.bag_tagger.scale_up_arn,
  ]

  queue_low_actions = [
    module.bag_tagger.scale_down_arn,
  ]

  dlq_alarm_arn = var.dlq_alarm_arn
}
