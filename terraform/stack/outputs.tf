output "api_gateway_id" {
  value = "${module.api.api_gateway_id}"
}

output "interservice_sg_id" {
  value = "${aws_security_group.interservice.id}"
}
