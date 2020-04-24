resource "aws_cloudwatch_metric_alarm" "alarm_5xx" {
  alarm_name          = "storage-api-${var.namespace}-5xx-alarm"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "5XXError"
  namespace           = "AWS/ApiGateway"
  period              = "60"
  statistic           = "Sum"
  threshold           = "0"

  dimensions = {
    ApiName = aws_api_gateway_rest_api.api.name
    Stage   = "v1"
  }

  alarm_actions = [var.alarm_topic_arn]
}

