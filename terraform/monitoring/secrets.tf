# Replicate read-only credentials for the reporting cluster into
# the storage account.
module "reporting_secrets" {
  source = "github.com/wellcomecollection/platform-infrastructure.git//critical/modules/secrets/distributed?ref=15a9446"

  secrets = {
    "reporting/es_host"               = "reporting/es_host"
    "reporting/read_only/es_password" = "reporting/read_only/es_password"
    "reporting/read_only/es_username" = "reporting/read_only/es_username"
  }
}
