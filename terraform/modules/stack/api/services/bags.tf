module "bags" {
  source = "../../../service/api"

  namespace = "${var.namespace}-bags-api"

  container_image = "${var.bags_container_image}"
  container_port  = "${var.bags_container_port}"

  namespace_id = "${var.namespace_id}"

  cluster_id = "${var.cluster_id}"

  vpc_id = "${var.vpc_id}"

  security_group_ids = [
    "${aws_security_group.service_egress_security_group.id}",
    "${aws_security_group.service_lb_ingress_security_group.id}",
    "${var.interservice_security_group_id}",
  ]

  subnets = ["${var.subnets}"]

  nginx_container_port  = "${var.bags_nginx_container_port}"
  nginx_container_image = "${var.bags_nginx_container_image}"

  env_vars = "${var.bags_env_vars}"

  env_vars_length = "${var.bags_env_vars_length}"

  lb_arn        = "${var.nlb_arn}"
  listener_port = "${var.bags_listener_port}"

  cpu    = 2048
  memory = 4096

  sidecar_cpu    = 1024
  sidecar_memory = 2048

  app_cpu    = 1024
  app_memory = 2048

  task_desired_count = "${var.desired_bags_api_count}"
}
