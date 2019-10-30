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

resource "aws_iam_role_policy" "s3_large_response_cache" {
  role   = "${module.api.bags_role_name}"
  policy = "${data.aws_iam_policy_document.s3_large_response_cache.json}"
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

# bag root finder

resource "aws_iam_role_policy" "bag_root_finder_read_s3_ingests" {
  role   = "${module.bag_root_finder.task_role_name}"
  policy = "${data.aws_iam_policy_document.ingests_read.json}"
}

resource "aws_iam_role_policy" "bag_root_finder_metrics" {
  role   = "${module.bag_root_finder.task_role_name}"
  policy = "${data.aws_iam_policy_document.cloudwatch_put.json}"
}

# bag versioner

resource "aws_iam_role_policy" "bag_versioner_metrics" {
  role   = "${module.bag_versioner.task_role_name}"
  policy = "${data.aws_iam_policy_document.cloudwatch_put.json}"
}

resource "aws_iam_role_policy" "bag_versioner_locking_table" {
  role   = "${module.bag_versioner.task_role_name}"
  policy = "${module.versioner_lock_table.iam_policy}"
}

resource "aws_iam_role_policy" "bag_versioner_versions_table" {
  role   = "${module.bag_versioner.task_role_name}"
  policy = "${data.aws_iam_policy_document.versioner_versions_table_table_readwrite.json}"
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

# The fetch files in the bag may refer to objects in the access bucket,
# so we need to grant this verifier read perms to that bucket as well.
resource "aws_iam_role_policy" "bag_verifier_pre_repl_read_replicator_bucket" {
  role   = "${module.bag_verifier_pre_replication.task_role_name}"
  policy = "${data.aws_iam_policy_document.storage_access_read.json}"
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

# replica aggregator

resource "aws_iam_role_policy" "replica_aggregator_post_repl_metrics" {
  role   = "${module.replica_aggregator.task_role_name}"
  policy = "${data.aws_iam_policy_document.cloudwatch_put.json}"
}

resource "aws_iam_role_policy" "replica_aggregator_replicas_table" {
  role   = "${module.replica_aggregator.task_role_name}"
  policy = "${data.aws_iam_policy_document.replicas_table_readwrite.json}"
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
