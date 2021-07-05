# bag_register

resource "aws_iam_role_policy" "bag_register_replica_primary_readonly" {
  role   = module.bag_register.task_role_name
  policy = data.aws_iam_policy_document.replica_primary_readonly.json
}

# bags_api

resource "aws_iam_role_policy" "bags_api_vhs_manifests_readonly" {
  role   = module.bags_api.task_role_name
  policy = var.vhs_manifests_readwrite_policy
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

# bag_tagger

resource "aws_iam_role_policy" "bag_tagger_can_tag_objects" {
  role   = module.bag_tagger.task_role_name
  policy = data.aws_iam_policy_document.allow_tagging_objects.json
}

# ingests_service

resource "aws_iam_role_policy" "ingests_table_readwrite" {
  role   = module.ingest_service.task_role_name
  policy = data.aws_iam_policy_document.table_ingests_readwrite.json
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

# bag versioner

resource "aws_iam_role_policy" "bag_versioner_locking_table" {
  role   = module.bag_versioner.task_role_name
  policy = module.working_storage.versioner_lock_iam_policy
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

resource "aws_iam_role_policy" "bag_verifier_pre_repl_unpacked_bags_bucket_put_tags" {
  role   = module.bag_verifier_pre_replication.task_role_name
  policy = data.aws_iam_policy_document.unpacked_bags_bucket_put_tags.json
}

# The fetch files in the bag may refer to objects in the primary bucket,
# so we need to grant this verifier read perms to that bucket as well.
resource "aws_iam_role_policy" "bag_verifier_pre_repl_replica_primary_readonly" {
  role   = module.bag_verifier_pre_replication.task_role_name
  policy = data.aws_iam_policy_document.replica_primary_readonly.json
}

# It also needs to be able to tag objects in the primary bucket, although
# eventually we should backfill the tags and do away with the need for this.
resource "aws_iam_role_policy" "bag_verifier_pre_repl_primary_replica_put_tags" {
  role   = module.bag_verifier_pre_replication.task_role_name
  policy = data.aws_iam_policy_document.primary_replica_put_tags.json
}

# bag_unpacker

resource "aws_iam_role_policy" "bag_unpacker_upload_buckets_readonly" {
  role   = module.bag_unpacker.task_role_name
  policy = data.aws_iam_policy_document.upload_buckets_readonly.json
}

resource "aws_iam_role_policy" "bag_unpacker_unpacked_bags_bucket_readwrite" {
  role   = module.bag_unpacker.task_role_name
  policy = data.aws_iam_policy_document.unpacked_bags_bucket_readwrite.json
}

# replica aggregator

resource "aws_iam_role_policy" "replica_aggregator_replicas_table" {
  role   = module.replica_aggregator.task_role_name
  policy = data.aws_iam_policy_document.table_replicas_readwrite.json
}

# Azure bag verifier

resource "aws_iam_role_policy" "allow_azure_verifier_access_cache" {
  count = local.azure_replicator_count

  role   = module.replicator_verifier_azure[count.index].verifier_task_role_name
  policy = data.aws_iam_policy_document.azure_verifier_tags_readwrite.json
}
