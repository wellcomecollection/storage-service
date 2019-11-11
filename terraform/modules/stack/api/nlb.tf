resource "aws_lb" "nlb" {
  name               = "${var.namespace}-api-nlb"
  internal           = true
  load_balancer_type = "network"
  subnets            = var.subnets
}
