# If you don't have this resource, you get an error when planning:
#
#     Error: error reading IAM Role (AWSServiceRoleForApplicationAutoScaling_ECSService):
#     NoSuchEntity:  The role with name AWSServiceRoleForApplicationAutoScaling_ECSService cannot be found.
#
resource "aws_iam_service_linked_role" "autoscaling_linked_role" {
  aws_service_name = "ecs.application-autoscaling.amazonaws.com"
}
