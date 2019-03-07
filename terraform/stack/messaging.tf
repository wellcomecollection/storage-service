locals {
  progress_topic = "${module.ingests_topic.arn}"
}

# Ingests topic.  Every app needs to be able to write to this, because they
# use it to send progress updates for the ingests API.

module "ingests_topic" {
  source = "../modules/topic"

  name = "${var.namespace}_ingests"

  role_names = [
    "${module.bag_register.task_role_name}",
    "${module.bag_replicator.task_role_name}",
    "${module.bag_verifier.task_role_name}",
    "${module.bag_unpacker.task_role_name}",
    "${module.ingests.task_role_name}",
    "${module.notifier.task_role_name}",
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

# bag_register

module "bag_register_input_queue" {
  source = "../modules/queue"

  name = "${var.namespace}_bags_input"

  topic_names = ["${module.bag_verifier_output_topic.name}"]

  role_names = ["${module.bag_register.task_role_name}"]

  aws_region    = "${var.aws_region}"
  dlq_alarm_arn = "${var.dlq_alarm_arn}"
}

# Messaging - notifier

module "ingests_output_topic" {
  source = "../modules/topic"

  name       = "${var.namespace}_ingests_output"
  role_names = ["${module.ingests.task_role_name}"]
}

module "notifier_input_queue" {
  source = "../modules/queue"

  name = "${var.namespace}_notifier"

  topic_names = ["${module.ingests_output_topic.name}"]

  role_names = ["${module.notifier.task_role_name}"]

  aws_region    = "${var.aws_region}"
  dlq_alarm_arn = "${var.dlq_alarm_arn}"
}

# Messaging - bagger

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

# Messaging - bag_unpacker

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

# Messaging - bag_verifier

module "bag_verifier_input_queue" {
  source = "../modules/queue"

  name = "${var.namespace}_bag_verifier_input"

  topic_names = ["${module.bag_unpacker_output_topic.name}"]

  role_names = ["${module.bag_verifier.task_role_name}"]

  # We keep a high visibility timeout to
  # avoid messages appearing to time out and fail.
  visibility_timeout_seconds = "${60 * 60 * 5}"

  max_receive_count = 1

  aws_region    = "${var.aws_region}"
  dlq_alarm_arn = "${var.dlq_alarm_arn}"
}


module "bag_verifier_output_topic" {
  source = "../modules/topic"

  name = "${var.namespace}_bag_verifier_output"

  role_names = [
    "${module.bag_verifier.task_role_name}",
  ]
}

module "bag_verifier_output_queue" {
  source = "../modules/queue"

  name = "${var.namespace}_bag_verifier_output"

  topic_names = ["${module.bag_verifier_output_topic.name}"]

  role_names = []

  # We keep a high visibility timeout to
  # avoid messages appearing to time out and fail.
  visibility_timeout_seconds = "${60 * 60 * 5}"

  max_receive_count = 1

  aws_region    = "${var.aws_region}"
  dlq_alarm_arn = "${var.dlq_alarm_arn}"
}

# bag_replicator

module "bag_replicator_input_queue" {
  source = "../modules/queue"

  name = "${var.namespace}_bag_replicator_input"

  topic_names = ["${module.bag_unpacker_output_topic.name}"]

  role_names = ["${module.bag_replicator.task_role_name}"]

  # Because these operations take a long time (potentially copying thousands
  # of S3 objects for a single message), we keep a high visibility timeout to
  # avoid messages appearing to time out and fail.
  visibility_timeout_seconds = "${60 * 60 * 5}"

  max_receive_count = 1

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