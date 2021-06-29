module "dlq_alarm" {
  source = "github.com/wellcomecollection/terraform-aws-sns-topic.git?ref=v1.0.0"
  name   = "shared_dlq_alarm"
}

module "gateway_server_error_alarm" {
  source = "github.com/wellcomecollection/terraform-aws-sns-topic.git?ref=v1.0.0"
  name   = "gateway_server_error_alarm"
}
