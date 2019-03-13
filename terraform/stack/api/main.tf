module "services" {
  source = "services"

  namespace = "${var.namespace}"

  namespace_id = "${var.namespace_id}"

  subnets    = ["${var.subnets}"]
  cluster_id = "${var.cluster_id}"
  vpc_id     = "${var.vpc_id}"
  nlb_arn    = "${module.nlb.arn}"

  // The number of api tasks MUST be one per AZ
  // This is due to the behaviour of NLBs that
  // seem to increase latency significantly if
  // number of tasks < number of AZs
  desired_bags_api_count    = "3"
  desired_ingests_api_count = "3"

  # Bags endpoint

  bags_container_image       = "${var.bags_container_image}"
  bags_container_port        = "${var.bags_container_port}"
  bags_env_vars              = "${var.bags_env_vars}"
  bags_env_vars_length       = "${var.bags_env_vars_length}"
  bags_nginx_container_image = "${var.bags_nginx_container_image}"
  bags_nginx_container_port  = "${var.bags_nginx_container_port}"
  bags_listener_port         = "${local.bags_listener_port}"

  # Ingests endpoint

  ingests_container_image                      = "${var.ingests_container_image}"
  ingests_container_port                       = "${var.ingests_container_port}"
  ingests_env_vars                             = "${var.ingests_env_vars}"
  ingests_env_vars_length                      = "${var.ingests_env_vars_length}"
  ingests_nginx_container_port                 = "${var.ingests_nginx_container_port}"
  ingests_nginx_container_image                = "${var.ingests_nginx_container_image}"
  ingests_listener_port                        = "${local.ingests_listener_port}"
  interservice_security_group_id               = "${var.interservice_security_group_id}"
  allow_ingests_publish_to_unpacker_topic_json = "${data.aws_iam_policy_document.allow_ingests_publish_to_unpacker_topic.json}"
}
