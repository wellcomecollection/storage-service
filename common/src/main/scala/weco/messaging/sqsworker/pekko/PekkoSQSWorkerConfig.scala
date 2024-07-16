package weco.messaging.sqsworker.pekko

import weco.messaging.sqs.SQSConfig
import weco.monitoring.MetricsConfig

case class PekkoSQSWorkerConfig(
  metricsConfig: MetricsConfig,
  sqsConfig: SQSConfig
)
