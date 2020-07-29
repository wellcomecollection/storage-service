locals {
  namespace = "storage"

  goobi_task_role_arn = "arn:aws:iam::299497370133:role/goobi_task_role"

  shell_server_1_task_role = "arn:aws:iam::299497370133:role/shell_server_1_task_role"
  shell_server_2_task_role = "arn:aws:iam::299497370133:role/shell_server_2_task_role"
  shell_server_3_task_role = "arn:aws:iam::299497370133:role/shell_server_3_task_role"
  shell_server_4_task_role = "arn:aws:iam::299497370133:role/shell_server_4_task_role"

  catalogue_pipeline_task_role_arn = "arn:aws:iam::760097843905:role/read_storage_s3_role"
  archivematica_task_role_arn      = "arn:aws:iam::299497370133:role/am-prod-storage-service_task_role"

  workflow_account_principal           = "arn:aws:iam::299497370133:root"
  digitisation_account_principal       = "arn:aws:iam::404315009621:root"
  catalogue_pipeline_account_principal = "arn:aws:iam::760097843905:root"

  digirati_account_principal = "arn:aws:iam::653428163053:root"
}
