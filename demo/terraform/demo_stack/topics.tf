# These SNS topics receive notifications from CloudWatch alarms,
# in particular:
#
#   - If we repeatedly fail to process a message on an SQS queue, the
#     message is eventually sent to a dead-letter queue (DLQ) using
#     an SQS redrive policy.
#
#   - 5XX server errors in the API Gateway distribution.
#
# Both of these events will trigger a CloudWatch alarm, which sends
# a notification to these topics.
#
# There's nothing in the demo stack that acts on these alarms.  We wire
# these up to a Lambda function that posts to an internal Slack channel,
# but this mechanism isn't in a fit state for reuse.

resource "aws_sns_topic" "dlq_alarm" {
  name = "shared_dlq_alarm"
}

resource "aws_sns_topic" "gateway_server_error_alarm" {
  name = "gateway_server_error_alarm"
}
