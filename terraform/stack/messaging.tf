# ingests

module "ingests_topic" {
  source = "../modules/topic"

  name = "${var.namespace}_ingests"

  role_names = [
    "${module.bag_register.task_role_name}",
    "${module.bag_replicator.task_role_name}",
    "${module.bag_root_finder.task_role_name}",
    "${module.bag_verifier_pre_replication.task_role_name}",
    "${module.bag_verifier_post_replication.task_role_name}",
    "${module.bag_unpacker.task_role_name}",
    "${module.ingests.task_role_name}",
    "${module.notifier.task_role_name}",
    "${module.bag_auditor.task_role_name}",
  ]
}

module "ingests_input_queue" {
  source = "../modules/queue"

  name = "${var.namespace}_ingests_input"

  topic_names = ["${module.ingests_topic.name}"]

  role_names = [
    "${module.ingests.task_role_name}",
  ]

  aws_region    = "${var.aws_region}"
  dlq_alarm_arn = "${var.dlq_alarm_arn}"
}

module "ingests_output_topic" {
  source = "../modules/topic"

  name       = "${var.namespace}_ingests_output"
  role_names = ["${module.ingests.task_role_name}"]
}

# notifier

module "notifier_input_queue" {
  source = "../modules/queue"

  name = "${var.namespace}_notifier"

  topic_names = ["${module.ingests_output_topic.name}"]

  role_names = ["${module.notifier.task_role_name}"]

  aws_region    = "${var.aws_region}"
  dlq_alarm_arn = "${var.dlq_alarm_arn}"
}

# bagger

module "bagger_topic" {
  source = "../modules/topic"

  name       = "${var.namespace}_bagger"
  role_names = []
}

module "bagger_queue" {
  source = "../modules/queue"

  name = "${var.namespace}_bagger"

  topic_names = ["${module.bagger_topic.name}"]

  role_names = ["${module.bagger.task_role_name}"]

  aws_region    = "${var.aws_region}"
  dlq_alarm_arn = "${var.dlq_alarm_arn}"
}

module "bagging_complete_topic" {
  source = "../modules/topic"

  name       = "${var.namespace}_bagging_complete"
  role_names = ["${module.bagger.task_role_name}"]
}

# bag_unpacker

module "bag_unpacker_input_topic" {
  source = "../modules/topic"

  name = "${var.namespace}_bag_unpacker_input"

  role_names = [
    "${module.bag_unpacker.task_role_name}",
  ]
}

module "bag_unpacker_queue" {
  source = "../modules/queue"

  name = "${var.namespace}_bag_unpacker_input"

  topic_names = ["${module.bag_unpacker_input_topic.name}"]

  role_names = ["${module.bag_unpacker.task_role_name}"]

  # We keep a high visibility timeout to
  # avoid messages appearing to time out and fail.
  visibility_timeout_seconds = "${60 * 60 * 5}"

  max_receive_count = 1

  queue_high_actions = [
    "${module.bag_unpacker.scale_up_arn}",
  ]

  queue_low_actions = [
    "${module.bag_unpacker.scale_down_arn}",
  ]

  aws_region    = "${var.aws_region}"
  dlq_alarm_arn = "${var.dlq_alarm_arn}"
}

module "bag_unpacker_output_topic" {
  source = "../modules/topic"

  name = "${var.namespace}_bag_unpacker_output"

  role_names = [
    "${module.bag_unpacker.task_role_name}",
  ]
}

# bag root finder

module "bag_root_finder_queue" {
  source = "../modules/queue"

  name = "${var.namespace}_bag_root_finder_input"

  topic_names = ["${module.bag_unpacker_output_topic.name}"]

  role_names = ["${module.bag_root_finder.task_role_name}"]

  queue_high_actions = [
    "${module.bag_root_finder.scale_up_arn}",
  ]

  queue_low_actions = [
    "${module.bag_root_finder.scale_down_arn}",
  ]

  aws_region    = "${var.aws_region}"
  dlq_alarm_arn = "${var.dlq_alarm_arn}"
}

module "bag_root_finder_output_topic" {
  source = "../modules/topic"

  name = "${var.namespace}_bag_root_finder_output"

  role_names = [
    "${module.bag_root_finder.task_role_name}",
  ]
}

# bag_verifier pre-replication

module "bag_verifier_pre_replicate_queue" {
  source = "../modules/queue"

  name = "${var.namespace}_bag_verifier_pre_replicate_input"

  topic_names = ["${module.bag_root_finder_output_topic.name}"]

  role_names = ["${module.bag_verifier_pre_replication.task_role_name}"]

  # We keep a high visibility timeout to
  # avoid messages appearing to time out and fail.
  visibility_timeout_seconds = "${60 * 60 * 5}"

  max_receive_count = 1

  queue_high_actions = [
    "${module.bag_verifier_pre_replication.scale_up_arn}",
  ]

  queue_low_actions = [
    "${module.bag_verifier_pre_replication.scale_down_arn}",
  ]

  aws_region    = "${var.aws_region}"
  dlq_alarm_arn = "${var.dlq_alarm_arn}"
}

module "bag_verifier_pre_replicate_output_topic" {
  source = "../modules/topic"

  name = "${var.namespace}_bag_verifier_pre_replicate_output"

  role_names = [
    "${module.bag_verifier_pre_replication.task_role_name}",
  ]
}

# bag auditor

module "bag_auditor_queue" {
  source = "../modules/queue"

  name = "${var.namespace}_bag_auditor_input"

  topic_names = ["${module.bag_verifier_pre_replicate_output_topic.name}"]

  role_names = ["${module.bag_auditor.task_role_name}"]

  queue_high_actions = [
    "${module.bag_auditor.scale_up_arn}",
  ]

  queue_low_actions = [
    "${module.bag_auditor.scale_down_arn}",
  ]

  aws_region    = "${var.aws_region}"
  dlq_alarm_arn = "${var.dlq_alarm_arn}"
}

module "bag_auditor_output_topic" {
  source = "../modules/topic"

  name = "${var.namespace}_bag_auditor_output"

  role_names = [
    "${module.bag_auditor.task_role_name}",
  ]
}

# bag_replicator

module "bag_replicator_input_queue" {
  source = "../modules/queue"

  name = "${var.namespace}_bag_replicator_input"

  topic_names = ["${module.bag_auditor_output_topic.name}"]

  role_names = ["${module.bag_replicator.task_role_name}"]

  # Because these operations take a long time (potentially copying thousands
  # of S3 objects for a single message), we keep a high visibility timeout to
  # avoid messages appearing to time out and fail.
  visibility_timeout_seconds = "${60 * 60 * 5}"

  max_receive_count = 1

  queue_high_actions = [
    "${module.bag_replicator.scale_up_arn}",
  ]

  queue_low_actions = [
    "${module.bag_replicator.scale_down_arn}",
  ]

  aws_region    = "${var.aws_region}"
  dlq_alarm_arn = "${var.dlq_alarm_arn}"
}

module "bag_replicator_output_topic" {
  source = "../modules/topic"

  name = "${var.namespace}_bag_replicator_output"

  role_names = [
    "${module.bag_replicator.task_role_name}",
  ]
}

# bag_verifier post-replication

module "bag_verifier_post_replicate_queue" {
  source = "../modules/queue"

  name = "${var.namespace}_bag_verifier_post_replicate_input"

  topic_names = ["${module.bag_replicator_output_topic.name}"]

  role_names = ["${module.bag_verifier_post_replication.task_role_name}"]

  # We keep a high visibility timeout to
  # avoid messages appearing to time out and fail.
  visibility_timeout_seconds = "${60 * 60 * 5}"

  max_receive_count = 1

  queue_high_actions = [
    "${module.bag_verifier_post_replication.scale_up_arn}",
  ]

  queue_low_actions = [
    "${module.bag_verifier_post_replication.scale_down_arn}",
  ]

  aws_region    = "${var.aws_region}"
  dlq_alarm_arn = "${var.dlq_alarm_arn}"
}

module "bag_verifier_post_replicate_output_topic" {
  source = "../modules/topic"

  name = "${var.namespace}_bag_verifier_post_replicate_output"

  role_names = [
    "${module.bag_verifier_post_replication.task_role_name}",
  ]
}

# bag_register

module "bag_register_input_queue" {
  source = "../modules/queue"

  name = "${var.namespace}_bag_register_input"

  topic_names = ["${module.bag_verifier_post_replicate_output_topic.name}"]

  role_names = ["${module.bag_register.task_role_name}"]

  queue_high_actions = [
    "${module.bag_register.scale_up_arn}",
  ]

  queue_low_actions = [
    "${module.bag_register.scale_down_arn}",
  ]

  aws_region    = "${var.aws_region}"
  dlq_alarm_arn = "${var.dlq_alarm_arn}"
}

module "bag_register_output_topic" {
  source = "../modules/topic"

  name = "${var.namespace}_bag_register_output"

  role_names = [
    "${module.bag_register.task_role_name}",
  ]
}

module "bag_register_output_queue" {
  source = "../modules/queue"

  name = "${var.namespace}_bag_register_output"

  topic_names = ["${module.bag_register_output_topic.name}"]

  role_names = []

  aws_region    = "${var.aws_region}"
  dlq_alarm_arn = "${var.dlq_alarm_arn}"
}
