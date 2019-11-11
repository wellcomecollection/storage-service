resource "aws_lb_target_group" "tcp" {
  name = var.namespace

  target_type = "ip"

  protocol = "TCP"
  port     = var.nginx_container_port
  vpc_id   = var.vpc_id

  health_check {
    protocol = "TCP"
  }
}

resource "aws_lb_listener" "tcp" {
  load_balancer_arn = var.lb_arn
  port              = var.listener_port
  protocol          = "TCP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.tcp.arn
  }
}
