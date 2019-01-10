data "aws_ssm_parameter" "bagger_dlcs_api_key" {
  name = "/storage/bagger/dlcs/api/key"
  with_decryption = "true"
}

locals {
  ssm_params = {
    "bagger_dlcs_api_key" = "${data.aws_ssm_parameter.bagger_dlcs_api_key.value}"
  }
}