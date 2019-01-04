# VPC Endpoint

resource "aws_vpc_endpoint_service" "service" {
  acceptance_required        = false
  network_load_balancer_arns = ["${module.nlb.arn}"]
  allowed_principals = ["${var.allowed_principals}"]
}

module "nlb" {
  source = "git::https://github.com/wellcometrust/terraform.git//load_balancer/network?ref=v14.2.0"

  namespace       = "${var.namespace}"
  private_subnets = ["${var.private_subnets}"]
}

# archivist

module "archivist-nvm" {
  source = "../modules/service/worker+nvm"

  service_egress_security_group_id = "${var.service_egress_security_group_id}"
  cluster_name                     = "${aws_ecs_cluster.cluster.name}"
  cluster_id                       = "${aws_ecs_cluster.cluster.id}"
  namespace_id                     = "${aws_service_discovery_private_dns_namespace.namespace.id}"
  subnets                          = "${var.private_subnets}"
  vpc_id                           = "${var.vpc_id}"
  service_name                     = "${var.namespace}-archivist-nvm"
  aws_region                       = "${var.aws_region}"

  env_vars = {
    queue_url           = "${module.archivist_queue.url}"
    archive_bucket      = "${var.archive_bucket_name}"
    registrar_topic_arn = "${module.bags_topic.arn}"
    progress_topic_arn  = "${module.ingests_topic.arn}"
  }

  env_vars_length = 4

  cpu    = "1900"
  memory = "14000"

  container_image = "${var.archivist_image}"
}

# bags aka registrar-async

module "bags" {
  source = "../modules/service/worker"

  service_egress_security_group_id = "${var.service_egress_security_group_id}"
  cluster_name                     = "${aws_ecs_cluster.cluster.name}"
  cluster_id                       = "${aws_ecs_cluster.cluster.id}"
  namespace_id                     = "${aws_service_discovery_private_dns_namespace.namespace.id}"
  subnets                          = "${var.private_subnets}"
  vpc_id                           = "${var.vpc_id}"
  service_name                     = "${var.namespace}-bags_async"
  aws_region                       = "${var.aws_region}"

  env_vars = {
    queue_url          = "${module.bags_queue.url}"
    archive_bucket     = "${var.archive_bucket_name}"
    progress_topic_arn = "${module.ingests_topic.arn}"
    vhs_bucket_name    = "${var.vhs_archive_manifest_bucket_name}"
    vhs_table_name     = "${var.vhs_archive_manifest_table_name}"
  }

  env_vars_length = 5

  container_image = "${var.bags_image}"
}

# notifier

module "notifier" {
  source = "../modules/service/worker"

  service_egress_security_group_id = "${var.service_egress_security_group_id}"

  security_group_ids = [
    "${var.interservice_security_group_id}",
    "${var.service_egress_security_group_id}",
  ]

  cluster_name = "${aws_ecs_cluster.cluster.name}"
  cluster_id   = "${aws_ecs_cluster.cluster.id}"
  namespace_id = "${aws_service_discovery_private_dns_namespace.namespace.id}"
  subnets      = "${var.private_subnets}"
  vpc_id       = "${var.vpc_id}"
  service_name = "${var.namespace}-notifier"
  aws_region   = "${var.aws_region}"

  env_vars = {
    context_url        = "https://api.wellcomecollection.org/storage/v1/context.json"
    notifier_queue_url = "${module.notifier_queue.url}"
    progress_topic_arn = "${module.ingests_topic.arn}"
  }

  env_vars_length = 3

  container_image = "${var.notifier_image}"
}

# ingests aka progress-async

module "ingests" {
  source = "../modules/service/worker"

  service_egress_security_group_id = "${var.service_egress_security_group_id}"
  cluster_name                     = "${aws_ecs_cluster.cluster.name}"
  cluster_id                       = "${aws_ecs_cluster.cluster.id}"

  namespace_id = "${aws_service_discovery_private_dns_namespace.namespace.id}"
  subnets      = "${var.private_subnets}"
  vpc_id       = "${var.vpc_id}"
  service_name = "${var.namespace}-ingests_async"
  aws_region   = "${var.aws_region}"

  env_vars = {
    queue_url                   = "${module.ingests_queue.url}"
    topic_arn                   = "${module.notifier_topic.arn}"
    archive_progress_table_name = "${var.ingests_table_name}"
  }

  env_vars_length = 3

  container_image = "${var.ingests_image}"
}

# Migration services

module "bagger-nvm" {
  source = "../modules/service/worker+nvm"

  service_egress_security_group_id = "${var.service_egress_security_group_id}"
  cluster_name                     = "${aws_ecs_cluster.cluster.name}"
  cluster_id                       = "${aws_ecs_cluster.cluster.id}"
  namespace_id                     = "${aws_service_discovery_private_dns_namespace.namespace.id}"
  subnets                          = "${var.private_subnets}"
  vpc_id                           = "${var.vpc_id}"
  service_name                     = "${var.namespace}-bagger-nvm"
  aws_region                       = "${var.aws_region}"

  env_vars = {
    METS_BUCKET_NAME            = "${var.bagger_mets_bucket_name}"
    READ_METS_FROM_FILESHARE    = "${var.bagger_read_mets_from_fileshare}"
    WORKING_DIRECTORY           = "${var.bagger_working_directory}"
    DROP_BUCKET_NAME            = "${var.bagger_drop_bucket_name}"
    DROP_BUCKET_NAME_METS_ONLY  = "${var.bagger_drop_bucket_name_mets_only}"
    DROP_BUCKET_NAME_ERRORS     = "${var.bagger_drop_bucket_name_errors}"
    CURRENT_PRESERVATION_BUCKET = "${var.bagger_current_preservation_bucket}"
    DLCS_SOURCE_BUCKET          = "${var.bagger_dlcs_source_bucket}"
    BAGGING_QUEUE               = "${module.bagger_queue.name}"
    BAGGING_COMPLETE_TOPIC_ARN  = "${module.bagging_complete_topic.arn}"

    AWS_DEFAULT_REGION = "${var.aws_region}"

    # DLCS config
    DLCS_ENTRY       = "${var.bagger_dlcs_entry}"
    DLCS_API_KEY     = "${var.bagger_dlcs_api_key}"
    DLCS_API_SECRET  = "${var.bagger_dlcs_api_secret}"
    DLCS_CUSTOMER_ID = "${var.bagger_dlcs_customer_id}"
    DLCS_SPACE       = "${var.bagger_dlcs_space}"

    # DDS credentials
    DDS_API_KEY      = "${var.bagger_dds_api_key}"
    DDS_API_SECRET   = "${var.bagger_dds_api_secret}"
    DDS_ASSET_PREFIX = "${var.bagger_dds_asset_prefix}"
  }

  env_vars_length = 19

  cpu    = "1900"
  memory = "14000"

  container_image = "${var.bagger_image}"
}

module "trigger_bag_ingest" {
  source = "trigger_bag_ingest"

  name                   = "${var.namespace}-trigger-bag-ingest"
  lambda_s3_key          = "lambdas/archive/lambdas/trigger_bag_ingest.zip"
  lambda_error_alarm_arn = "${var.lambda_error_alarm_arn}"
  infra_bucket           = "${var.infra_bucket}"
  oauth_details_enc      = "${var.archive_oauth_details_enc}"
  bag_paths              = "${var.bag_paths}"
  ingest_bucket_name     = "${var.ingest_bucket_name}"
  account_id             = "${var.account_id}"

  use_encryption_key_policy = "${var.use_encryption_key_policy}"
}
