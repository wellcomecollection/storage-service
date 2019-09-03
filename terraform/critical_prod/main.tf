module "critical" {
  source = "../critical"

  namespace  = "${local.namespace}"
  account_id = "${local.account_id}"

  archive_read_principles = [
    "${local.goobi_task_role_arn}",
    "${local.archivematica_task_role_arn}",
    "${data.aws_iam_user.dds_digirati.arn}",
    "${local.digitisation_account_principal}",
    "${local.workflow_account_principal}",
  ]

  access_read_principles = [
    "${local.goobi_task_role_arn}",
    "${local.archivematica_task_role_arn}",
    "${data.aws_iam_user.dds_digirati.arn}",
    "arn:aws:iam::653428163053:user/echo-fs",
    "arn:aws:iam::653428163053:user/api",
    "${local.digitisation_account_principal}",
    "${local.digitisation_mediaconvert_role_arn}",
    "${local.workflow_account_principal}",
  ]

  ingest_read_principles = [
    "${local.goobi_task_role_arn}",
    "${local.archivematica_task_role_arn}",
    "${local.digitisation_account_principal}",
    "${local.workflow_account_principal}",
  ]
}
