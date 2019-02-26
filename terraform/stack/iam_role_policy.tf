# bags aka registrar-async

resource "aws_iam_role_policy" "bags_archive_get" {
  role   = "${module.bags.task_role_name}"
  policy = "${data.aws_iam_policy_document.storage_archive_read.json}"
}

resource "aws_iam_role_policy" "bags_vhs_write" {
  role   = "${module.bags.task_role_name}"
  policy = "${var.vhs_archive_manifest_full_access_policy_json}"
}

resource "aws_iam_role_policy" "bags_metrics" {
  role   = "${module.bags.task_role_name}"
  policy = "${data.aws_iam_policy_document.cloudwatch_put.json}"
}

# archivist

resource "aws_iam_role_policy" "archivist_task_store_s3" {
  role   = "${module.archivist.task_role_name}"
  policy = "${data.aws_iam_policy_document.storage_archive_readwrite.json}"
}

resource "aws_iam_role_policy" "archivist_task_get_s3" {
  role   = "${module.archivist.task_role_name}"
  policy = "${data.aws_iam_policy_document.ingests_read.json}"
}

resource "aws_iam_role_policy" "archivist_task_get_s3_bagger" {
  role   = "${module.archivist.task_role_name}"
  policy = "${data.aws_iam_policy_document.ingests_read.json}"
}

resource "aws_iam_role_policy" "archivist_task_get_s3_workflow" {
  role   = "${module.archivist.task_role_name}"
  policy = "${data.aws_iam_policy_document.ingests_read.json}"
}

resource "aws_iam_role_policy" "archivist_metrics" {
  role   = "${module.archivist.task_role_name}"
  policy = "${data.aws_iam_policy_document.cloudwatch_put.json}"
}

# api.bags aka registrar-http

resource "aws_iam_role_policy" "bags_vhs" {
  role   = "${module.api.bags_role_name}"
  policy = "${var.vhs_archive_manifest_read_policy_json}"
}

resource "aws_iam_role_policy" "bags_api_metrics" {
  role   = "${module.api.bags_role_name}"
  policy = "${data.aws_iam_policy_document.cloudwatch_put.json}"
}

# api.ingests aka progress-http

resource "aws_iam_role_policy" "ingests_api_archive_progress_table" {
  role   = "${module.api.ingests_role_name}"
  policy = "${data.aws_iam_policy_document.archive_progress_table_read_write_policy.json}"
}

# ingests aka progress-async

resource "aws_iam_role_policy" "ingests_archive_progress_table" {
  role   = "${module.ingests.task_role_name}"
  policy = "${data.aws_iam_policy_document.archive_progress_table_read_write_policy.json}"
}

resource "aws_iam_role_policy" "ingests_metrics" {
  role   = "${module.ingests.task_role_name}"
  policy = "${data.aws_iam_policy_document.cloudwatch_put.json}"
}

# ingests_api aka progress-http

resource "aws_iam_role_policy" "ingests_api_metrics" {
  role   = "${module.api.ingests_role_name}"
  policy = "${data.aws_iam_policy_document.cloudwatch_put.json}"
}

# bag_replicator

resource "aws_iam_role_policy" "bag_replicator_task_read_s3" {
  role   = "${module.bag_replicator.task_role_name}"
  policy = "${data.aws_iam_policy_document.storage_archive_read.json}"
}

resource "aws_iam_role_policy" "bag_replicator_task_store_s3" {
  role   = "${module.bag_replicator.task_role_name}"
  policy = "${data.aws_iam_policy_document.storage_access_readwrite.json}"
}

resource "aws_iam_role_policy" "bag_replicator_metrics" {
  role   = "${module.bag_replicator.task_role_name}"
  policy = "${data.aws_iam_policy_document.cloudwatch_put.json}"
}

# bag_verifier

resource "aws_iam_role_policy" "bag_verifier_task_read_s3" {
  role   = "${module.bag_verifier.task_role_name}"
  policy = "${data.aws_iam_policy_document.storage_archive_read.json}"
}

resource "aws_iam_role_policy" "bag_verifier_task_store_s3" {
  role   = "${module.bag_verifier.task_role_name}"
  policy = "${data.aws_iam_policy_document.storage_access_read.json}"
}

resource "aws_iam_role_policy" "bag_verifier_metrics" {
  role   = "${module.bag_verifier.task_role_name}"
  policy = "${data.aws_iam_policy_document.cloudwatch_put.json}"
}

# notifier

resource "aws_iam_role_policy" "notifier_metrics" {
  role   = "${module.notifier.task_role_name}"
  policy = "${data.aws_iam_policy_document.cloudwatch_put.json}"
}

# bagger

resource "aws_iam_role_policy" "bagger_progress_table_readwrite" {
  role   = "${module.bagger.task_role_name}"
  policy = "${data.aws_iam_policy_document.bagger_progress_table_readwrite.json}"
}

resource "aws_iam_role_policy" "bagger_task_queue_discovery" {
  role   = "${module.bagger.task_role_name}"
  policy = "${data.aws_iam_policy_document.bagger_queue_discovery.json}"
}

resource "aws_iam_role_policy" "bagger_task_get_s3" {
  role   = "${module.bagger.task_role_name}"
  policy = "${data.aws_iam_policy_document.bagger_read.json}"
}

resource "aws_iam_role_policy" "bagger_task_put_s3" {
  role   = "${module.bagger.task_role_name}"
  policy = "${data.aws_iam_policy_document.bagger_readwrite.json}"
}

resource "aws_iam_role_policy" "bagger_task_get_s3_dlcs" {
  role   = "${module.bagger.task_role_name}"
  policy = "${data.aws_iam_policy_document.bagger_dlcs_read.json}"
}

resource "aws_iam_role_policy" "bagger_task_get_s3_preservica" {
  role   = "${module.bagger.task_role_name}"
  policy = "${data.aws_iam_policy_document.bagger_preservica_read.json}"
}

resource "aws_iam_role_policy" "bagger_s3" {
  role   = "${module.bagger.task_role_name}"
  policy = "${data.aws_iam_policy_document.bagger_s3_readwrite.json}"
}
