module "critical" {
  source = "../modules/critical"

  namespace  = "${local.namespace}-staging"
  account_id = "${local.account_id}"

  archive_read_principles = [
    "${local.goobi_task_role_arn}",
    "${local.archivematica_task_role_arn}",
    "${local.digitisation_account_principal}",
    "${local.workflow_account_principal}",
  ]

  replica_primary_read_principals = [
    "${local.archivematica_task_role_arn}",
    "${local.digitisation_account_principal}",
    "${local.digitisation_mediaconvert_role_arn}",
    "${local.goobi_task_role_arn}",
    "${local.workflow_account_principal}",
    "arn:aws:iam::653428163053:user/api",
    "arn:aws:iam::653428163053:user/echo-fs",
  ]

  ingest_read_principles = [
    "${local.goobi_task_role_arn}",
    "${local.archivematica_task_role_arn}",
    "${local.digitisation_account_principal}",
    "${local.workflow_account_principal}",
  ]
}
