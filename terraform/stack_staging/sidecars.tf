# Sidecar image tags are set centrally in platform-infrastructure/sidecars/terraform.
data "aws_ssm_parameter" "fluentbit_tag" {
  name = "/platform/sidecars/fluentbit/stage"
}

data "aws_ssm_parameter" "nginx_tag" {
  name = "/platform/sidecars/nginx/stage"
}
