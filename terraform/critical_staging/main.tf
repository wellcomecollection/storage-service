module "critical" {
  source = "../modules/critical"

  namespace = "${local.namespace}-staging"

  azure_resource_group_name = "rg-wcollarchive-stage"

  replica_primary_read_principals = [
    local.archivematica_task_role_arn,
    local.digitisation_account_principal,
    local.goobi_task_role_arn,

    local.shell_server_1_task_role,
    local.shell_server_2_task_role,
    local.shell_server_3_task_role,
    local.shell_server_4_task_role,

    local.workflow_account_principal,
    local.catalogue_pipeline_task_role_arn,
    local.catalogue_pipeline_account_principal,

    "arn:aws:iam::653428163053:user/api",
    "arn:aws:iam::653428163053:user/echo-fs",
  ]

  inventory_bucket = "wellcomecollection-storage-infra"

  tags = local.default_tags

  table_name = "vhs-storage-staging-manifests-25062020"

  # The staging service shouldn't be the only copy of any important data, so
  # we don't need S3 versioning.
  enable_s3_versioning = false
}