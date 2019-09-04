data "aws_caller_identity" "current" {}

locals {
  namespace = "storage"

  account_id = "${data.aws_caller_identity.current.account_id}"

  goobi_task_role_arn         = "arn:aws:iam::299497370133:role/goobi_task_role"
  archivematica_task_role_arn = "arn:aws:iam::299497370133:role/am-storage-service_task_role"

  workflow_account_principal     = "arn:aws:iam::299497370133:root"
  digitisation_account_principal = "arn:aws:iam::404315009621:root"

  digitisation_mediaconvert_role_arn = "${data.terraform_remote_state.digitisation_private.mediaconvert_role_arn}"
}