# bag_register

resource "aws_iam_role_policy" "bag_register_archive_get" {
  role   = "${module.bag_register.task_role_name}"
  policy = "${data.aws_iam_policy_document.storage_archive_read.json}"
}

resource "aws_iam_role_policy" "bag_register_access_get" {
  role   = "${module.bag_register.task_role_name}"
  policy = "${data.aws_iam_policy_document.storage_access_read.json}"
}

resource "aws_iam_role_policy" "bag_register_vhs_write" {
  role   = "${module.bag_register.task_role_name}"
  policy = "${var.vhs_archive_manifest_full_access_policy_json}"
}

resource "aws_iam_role_policy" "bag_register_metrics" {
  role   = "${module.bag_register.task_role_name}"
  policy = "${data.aws_iam_policy_document.cloudwatch_put.json}"
}

# bags_api

resource "aws_iam_role_policy" "bags_vhs" {
  role   = "${module.api.bags_role_name}"
  policy = "${var.vhs_archive_manifest_read_policy_json}"
}

resource "aws_iam_role_policy" "bags_api_metrics" {
  role   = "${module.api.bags_role_name}"
  policy = "${data.aws_iam_policy_document.cloudwatch_put.json}"
}

# ingests

resource "aws_iam_role_policy" "ingests_archive_ingest_table" {
  role   = "${module.ingests.task_role_name}"
  policy = "${data.aws_iam_policy_document.archive_ingest_table_read_write_policy.json}"
}

resource "aws_iam_role_policy" "ingests_metrics" {
  role   = "${module.ingests.task_role_name}"
  policy = "${data.aws_iam_policy_document.cloudwatch_put.json}"
}

# ingests_api

resource "aws_iam_role_policy" "ingests_api_metrics" {
  role   = "${module.api.ingests_role_name}"
  policy = "${data.aws_iam_policy_document.cloudwatch_put.json}"
}

resource "aws_iam_role_policy" "ingests_api_archive_ingest_table" {
  role   = "${module.api.ingests_role_name}"
  policy = "${data.aws_iam_policy_document.archive_ingest_table_read_write_policy.json}"
}

# bag auditor

resource "aws_iam_role_policy" "bag_auditor_read_s3_ingests" {
  role   = "${module.bag_auditor.task_role_name}"
  policy = "${data.aws_iam_policy_document.ingests_read.json}"
}

resource "aws_iam_role_policy" "bag_auditor_metrics" {
  role   = "${module.bag_auditor.task_role_name}"
  policy = "${data.aws_iam_policy_document.cloudwatch_put.json}"
}

resource "aws_iam_role_policy" "bag_auditor_locking_table" {
  role   = "${module.bag_auditor.task_role_name}"
  policy = "${module.auditor_lock_table.iam_policy}"
}

resource "aws_iam_role_policy" "bag_auditor_locking_table" {
  role   = "${module.bag_auditor.task_role_name}"
  policy = "${module.auditor_versions_table_table_readwrite.iam_policy}"
}

# bag_verifier pre-replication

resource "aws_iam_role_policy" "bag_verifier_pre_repl_read_s3_ingests" {
  role   = "${module.bag_verifier_pre_replication.task_role_name}"
  policy = "${data.aws_iam_policy_document.ingests_read.json}"
}

resource "aws_iam_role_policy" "bag_verifier_pre_repl_metrics" {
  role   = "${module.bag_verifier_pre_replication.task_role_name}"
  policy = "${data.aws_iam_policy_document.cloudwatch_put.json}"
}

# bag_replicator

resource "aws_iam_role_policy" "bag_replicator_task_read_ingests_s3" {
  role   = "${module.bag_replicator.task_role_name}"
  policy = "${data.aws_iam_policy_document.ingests_read.json}"
}

resource "aws_iam_role_policy" "bag_replicator_task_store_s3" {
  role   = "${module.bag_replicator.task_role_name}"
  policy = "${data.aws_iam_policy_document.storage_access_readwrite.json}"
}

resource "aws_iam_role_policy" "bag_replicator_metrics" {
  role   = "${module.bag_replicator.task_role_name}"
  policy = "${data.aws_iam_policy_document.cloudwatch_put.json}"
}

resource "aws_iam_role_policy" "bag_replicator_locking_table" {
  role   = "${module.bag_replicator.task_role_name}"
  policy = "${module.replicator_lock_table.iam_policy}"
}

# bag_verifier post-replication

resource "aws_iam_role_policy" "bag_verifier_post_repl_read_s3_archive" {
  role   = "${module.bag_verifier_post_replication.task_role_name}"
  policy = "${data.aws_iam_policy_document.storage_archive_read.json}"
}

resource "aws_iam_role_policy" "bag_verifier_post_repl_read_s3_access" {
  role   = "${module.bag_verifier_post_replication.task_role_name}"
  policy = "${data.aws_iam_policy_document.storage_access_read.json}"
}

resource "aws_iam_role_policy" "bag_verifier_post_repl_metrics" {
  role   = "${module.bag_verifier_post_replication.task_role_name}"
  policy = "${data.aws_iam_policy_document.cloudwatch_put.json}"
}

# bag_unpacker

resource "aws_iam_role_policy" "bag_unpacker_task_read_ingests_s3" {
  role   = "${module.bag_unpacker.task_role_name}"
  policy = "${data.aws_iam_policy_document.ingests_read.json}"
}

resource "aws_iam_role_policy" "bag_unpacker_task_readwrite_ingests_drop_s3" {
  role   = "${module.bag_unpacker.task_role_name}"
  policy = "${data.aws_iam_policy_document.storage_ingests_drop_read_write.json}"
}

resource "aws_iam_role_policy" "bag_unpacker_metrics" {
  role   = "${module.bag_unpacker.task_role_name}"
  policy = "${data.aws_iam_policy_document.cloudwatch_put.json}"
}

resource "aws_iam_role_policy" "bag_unpacker_get_archivematica_ingests" {
  role   = "${module.bag_unpacker.task_role_name}"
  policy = "${data.aws_iam_policy_document.archivematica_ingests_get.json}"
}

data "aws_iam_policy_document" "archivematica_ingests_get" {
  statement {
    actions = [
      "s3:Get*",
    ]

    resources = [
      "arn:aws:s3:::${var.archivematica_ingests_bucket}/*",
    ]
  }
}

# notifier

resource "aws_iam_role_policy" "notifier_metrics" {
  role   = "${module.notifier.task_role_name}"
  policy = "${data.aws_iam_policy_document.cloudwatch_put.json}"
}

# bagger

resource "aws_iam_role_policy" "bagger_ingest_table_readwrite" {
  role   = "${module.bagger.task_role_name}"
  policy = "${data.aws_iam_policy_document.bagger_ingest_table_readwrite.json}"
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

resource "aws_iam_role_policy" "bagger_s3_cache" {
  role   = "${module.bagger.task_role_name}"
  policy = "${data.aws_iam_policy_document.storage_bagger_cache_drop_read_write.json}"
}
