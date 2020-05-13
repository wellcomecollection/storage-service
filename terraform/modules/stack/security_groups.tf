resource "aws_security_group" "service_egress" {
  name        = "${var.namespace}_service_egress"
  description = "Allow traffic between services"
  vpc_id      = var.vpc_id

  egress {
    from_port = 0
    to_port   = 0
    protocol  = "-1"

    cidr_blocks = [
      "0.0.0.0/0",
    ]
  }

  tags = {
    Name = "${var.namespace}-egress"
  }
}

resource "aws_security_group" "interservice" {
  name        = "${var.namespace}_interservice"
  description = "Allow traffic between services"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = [data.aws_vpc.vpc.cidr_block]
  }

  tags = {
    Name = "${var.namespace}-interservice"
  }
}

data "aws_vpc" "vpc" {
  id = var.vpc_id
}