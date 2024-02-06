resource "aws_lb_target_group" "tcp" {
  name = var.service_name

  target_type = "ip"

  protocol = "TCP"
  port     = module.nginx_container.container_port
  vpc_id   = var.vpc_id

  # The default deregistration delay is 5 minutes, which means that ECS
  # takes around 5–7 mins to fully drain connections to and deregister
  # the old task in the course of its blue/green. deployment of an
  # updated service.  Reducing this parameter to 90s makes deployments faster.
  deregistration_delay = 90

  health_check {
    protocol = "HTTP"
    path     = var.healthcheck_path
    matcher  = "200"
  }
}

resource "aws_lb_listener" "tcp" {
  load_balancer_arn = var.load_balancer_arn
  port              = var.load_balancer_listener_port
  protocol          = "TCP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.tcp.arn
  }
}
