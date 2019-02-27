# Ingests topic.  Every app needs to be able to write to this, because they
# use it to send progress updates for the ingests API.

module "ingests_topic" {
  source = "../modules/topic"

  name = "${var.namespace}_ingests"

  role_names = [
    "${module.archivist.task_role_name}",
    "${module.bags.task_role_name}",
    "${module.bag_replicator.task_role_name}",
    "${module.bag_verifier.task_role_name}",
    "${module.ingests.task_role_name}",
    "${module.notifier.task_role_name}",
  ]
}

module "ingests_queue" {
  source = "../modules/queue"

  name = "${var.namespace}_ingests"

  topic_names = ["${module.ingests_topic.name}"]

  aws_region = "${var.aws_region}"
  account_id = "${var.current_account_id}"

  role_names = [
    "${module.ingests.task_role_name}",
  ]

  dlq_alarm_arn = "${var.dlq_alarm_arn}"
}

# Messaging - archivist

module "ingest_requests_topic" {
  source = "../modules/topic"

  name       = "${var.namespace}_ingest_requests"
  role_names = ["${module.api.ingests_role_name}"]
}

module "archivist_queue" {
  source = "../modules/queue"

  name = "${var.namespace}_archivist"

  topic_names = [
    "${module.ingest_requests_topic.name}",
  ]

  aws_region = "${var.aws_region}"
  account_id = "${var.current_account_id}"

  role_names = [
    "${module.archivist.task_role_name}",
  ]

  visibility_timeout_seconds = "3600"
  max_receive_count          = "1"

  dlq_alarm_arn = "${var.dlq_alarm_arn}"
}

# Messaging - bags aka registrar-async

module "bags_topic" {
  source = "../modules/topic"

  name = "${var.namespace}_bags"

  role_names = [
    "${module.bag_replicator.task_role_name}",
  ]
}

module "bags_queue" {
  source = "../modules/queue"

  name = "${var.namespace}_bags"

  topic_names = ["${module.bags_topic.name}"]

  aws_region = "${var.aws_region}"
  account_id = "${var.current_account_id}"
  role_names = ["${module.bags.task_role_name}"]

  dlq_alarm_arn = "${var.dlq_alarm_arn}"
}

# Messaging - notifier

module "notifier_topic" {
  source = "../modules/topic"

  name       = "${var.namespace}_notifier"
  role_names = ["${module.ingests.task_role_name}"]
}

module "notifier_queue" {
  source = "../modules/queue"

  name = "${var.namespace}_notifier"

  topic_names = ["${module.notifier_topic.name}"]

  aws_region = "${var.aws_region}"
  account_id = "${var.current_account_id}"
  role_names = ["${module.notifier.task_role_name}"]

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

  aws_region = "${var.aws_region}"
  account_id = "${var.current_account_id}"
  role_names = ["${module.bagger.task_role_name}"]

  dlq_alarm_arn = "${var.dlq_alarm_arn}"
}

module "bagging_complete_topic" {
  source = "../modules/topic"

  name       = "${var.namespace}_bagging_complete"
  role_names = ["${module.bagger.task_role_name}"]
}

# Messaging - bag_replicator

module "bag_replicator_topic" {
  source = "../modules/topic"

  name = "${var.namespace}_bag_replicator"

  role_names = [
    "${module.archivist.task_role_name}",
  ]
}

module "bag_replicator_queue" {
  source = "../modules/queue"

  name = "${var.namespace}_bag_replicator"

  topic_names = ["${module.bag_replicator_topic.name}"]

  aws_region = "${var.aws_region}"
  account_id = "${var.current_account_id}"
  role_names = ["${module.bag_replicator.task_role_name}"]

  dlq_alarm_arn = "${var.dlq_alarm_arn}"

  # Because these operations take a long time (potentially copying thousands
  # of S3 objects for a single message), we keep a high visibility timeout to
  # avoid messages appearing to time out and fail.
  visibility_timeout_seconds = "${60 * 60 * 5}"

  max_receive_count = 1
}

# Messaging - bag_verifier

# This service is currently being "tap" tested
# from the output of the Archivist service
# It does not have it's own topic

module "bag_verifier_queue" {
  source = "../modules/queue"

  name = "${var.namespace}_bag_verifier"

  topic_names = ["${module.bags_topic.name}"]

  aws_region = "${var.aws_region}"
  account_id = "${var.current_account_id}"
  role_names = ["${module.bag_verifier.task_role_name}"]

  dlq_alarm_arn = "${var.dlq_alarm_arn}"

  # We keep a high visibility timeout to
  # avoid messages appearing to time out and fail.
  visibility_timeout_seconds = "${60 * 60 * 5}"

  max_receive_count = 1
}

# Services using the null topic are sending their
# output nowhere in particular (a bit like /dev/null)

module "null_topic" {
  source = "../modules/topic"

  name = "${var.namespace}_null_topic"

  role_names = [
    "${module.bag_verifier.task_role_name}",
  ]
}
