module "critical" {
  source = "../modules/critical"

  namespace = local.namespace

  replica_primary_read_principals = [
    local.archivematica_task_role_arn,
    local.digitisation_account_principal,
    local.digitisation_mediaconvert_role_arn,
    local.goobi_task_role_arn,

    "${local.shell_server_1_task_role}",
    "${local.shell_server_2_task_role}",
    "${local.shell_server_3_task_role}",
    "${local.shell_server_4_task_role}",

    local.workflow_account_principal,
    local.catalogue_pipeline_task_role_arn,
    local.catalogue_pipeline_account_principal,

    "arn:aws:iam::653428163053:user/api",
    "arn:aws:iam::653428163053:user/echo-fs",
  ]

  replica_glacier_read_principals = []
}

