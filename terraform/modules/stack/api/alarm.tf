resource "aws_cloudwatch_metric_alarm" "alarm_5xx" {
  alarm_name          = "storage-api-${var.namespace}-5xx-alarm"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "5XXError"
  namespace           = "AWS/ApiGateway"
  period              = "60"
  statistic           = "Sum"
  threshold           = "0"
  alarm_description   = "[component=storage-bags-api severity=${var.alarm_severity}] Storage API Gateway is returning 5xx errors"

  dimensions = {
    ApiName = aws_api_gateway_rest_api.api.name
    Stage   = "v1"
  }

  alarm_actions = [var.alarm_topic_arn]

  tags = {
    "backplane:component"     = "storage-bags-api"
    "github.com/project-slug" = "wellcomecollection/storage-service"
    severity                  = var.alarm_severity
  }
}

