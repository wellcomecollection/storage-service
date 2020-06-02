# bag_register

resource "aws_iam_role_policy" "bag_register_replica_primary_readonly" {
  role   = module.bag_register.task_role_name
  policy = data.aws_iam_policy_document.replica_primary_readonly.json
}

resource "aws_iam_role_policy" "bag_register_metrics" {
  role   = module.bag_register.task_role_name
  policy = data.aws_iam_policy_document.cloudwatch_putmetrics.json
}

# bags_api

resource "aws_iam_role_policy" "bags_api_vhs_manifests_readonly" {
  role   = module.bags_api.task_role_name
  policy = var.vhs_manifests_readwrite_policy
}

resource "aws_iam_role_policy" "bags_api_metrics" {
  role   = module.bags_api.task_role_name
  policy = data.aws_iam_policy_document.cloudwatch_putmetrics.json
}

resource "aws_iam_role_policy" "s3_large_response_cache" {
  role   = module.bags_api.task_role_name
  policy = data.aws_iam_policy_document.s3_large_response_cache.json
}

# bag_indexer

resource "aws_iam_role_policy" "bag_indexer_vhs_manifests_readonly" {
  role   = module.bag_indexer.task_role_name
  policy = var.vhs_manifests_readonly_policy
}

resource "aws_iam_role_policy" "bag_indexer_metrics" {
  role   = module.bag_indexer.task_role_name
  policy = data.aws_iam_policy_document.cloudwatch_putmetrics.json
}

# ingests_indexer

resource "aws_iam_role_policy" "ingests_indexer_metrics" {
  role   = module.ingests_indexer.task_role_name
  policy = data.aws_iam_policy_document.cloudwatch_putmetrics.json
}

# ingests_service

resource "aws_iam_role_policy" "ingests_table_readwrite" {
  role   = module.ingest_service.task_role_name
  policy = data.aws_iam_policy_document.table_ingests_readwrite.json
}

resource "aws_iam_role_policy" "ingests_metrics" {
  role   = module.ingest_service.task_role_name
  policy = data.aws_iam_policy_document.cloudwatch_putmetrics.json
}

resource "aws_iam_role_policy" "ingests_service_metrics" {
  role   = module.ingest_service.task_role_name
  policy = data.aws_iam_policy_document.cloudwatch_putmetrics.json
}

resource "aws_iam_role_policy" "ingests_service_table_readwrite" {
  role   = module.ingest_service.task_role_name
  policy = data.aws_iam_policy_document.table_ingests_readwrite.json
}

# bag root finder

resource "aws_iam_role_policy" "bag_root_finder_unpacked_bags_bucket_readonly" {
  role   = module.bag_root_finder.task_role_name
  policy = data.aws_iam_policy_document.unpacked_bags_bucket_readonly.json
}

resource "aws_iam_role_policy" "bag_root_finder_metrics" {
  role   = module.bag_root_finder.task_role_name
  policy = data.aws_iam_policy_document.cloudwatch_putmetrics.json
}

# bag versioner

resource "aws_iam_role_policy" "bag_versioner_metrics" {
  role   = module.bag_versioner.task_role_name
  policy = data.aws_iam_policy_document.cloudwatch_putmetrics.json
}

resource "aws_iam_role_policy" "bag_versioner_locking_table" {
  role   = module.bag_versioner.task_role_name
  policy = module.versioner_lock_table.iam_policy
}

resource "aws_iam_role_policy" "bag_versioner_versions_table" {
  role   = module.bag_versioner.task_role_name
  policy = data.aws_iam_policy_document.versioner_versions_table_table_readwrite.json
}

# bag_verifier pre-replication

resource "aws_iam_role_policy" "bag_verifier_pre_repl_unpacked_bags_bucket_readonly" {
  role   = module.bag_verifier_pre_replication.task_role_name
  policy = data.aws_iam_policy_document.unpacked_bags_bucket_readonly.json
}

resource "aws_iam_role_policy" "bag_verifier_pre_repl_metrics" {
  role   = module.bag_verifier_pre_replication.task_role_name
  policy = data.aws_iam_policy_document.cloudwatch_putmetrics.json
}

# The fetch files in the bag may refer to objects in the primary bucket,
# so we need to grant this verifier read perms to that bucket as well.
resource "aws_iam_role_policy" "bag_verifier_pre_repl_replica_primary_readonly" {
  role   = module.bag_verifier_pre_replication.task_role_name
  policy = data.aws_iam_policy_document.replica_primary_readonly.json
}

# bag_unpacker

resource "aws_iam_role_policy" "bag_unpacker_drop_buckets_readonly" {
  role   = module.bag_unpacker.task_role_name
  policy = data.aws_iam_policy_document.drop_buckets_readonly.json
}

resource "aws_iam_role_policy" "bag_unpacker_unpacked_bags_bucket_readwrite" {
  role   = module.bag_unpacker.task_role_name
  policy = data.aws_iam_policy_document.unpacked_bags_bucket_readwrite.json
}

resource "aws_iam_role_policy" "bag_unpacker_metrics" {
  role   = module.bag_unpacker.task_role_name
  policy = data.aws_iam_policy_document.cloudwatch_putmetrics.json
}

resource "aws_iam_role_policy" "bag_unpacker_get_archivematica_ingests" {
  role   = module.bag_unpacker.task_role_name
  policy = data.aws_iam_policy_document.archivematica_ingests_get.json
}

# replica aggregator

resource "aws_iam_role_policy" "replica_aggregator_post_repl_metrics" {
  role   = module.replica_aggregator.task_role_name
  policy = data.aws_iam_policy_document.cloudwatch_putmetrics.json
}

resource "aws_iam_role_policy" "replica_aggregator_replicas_table" {
  role   = module.replica_aggregator.task_role_name
  policy = data.aws_iam_policy_document.table_replicas_readwrite.json
}

# notifier

resource "aws_iam_role_policy" "notifier_metrics" {
  role   = module.notifier.task_role_name
  policy = data.aws_iam_policy_document.cloudwatch_putmetrics.json
}

