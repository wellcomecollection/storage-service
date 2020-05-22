resource "aws_lb_target_group" "tcp" {
  name = var.service_name

  target_type = "ip"

  protocol = "TCP"
  port     = module.nginx_container.container_port
  vpc_id   = var.vpc_id

  health_check {
    protocol = "TCP"
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
