package weco.messaging.sqsworker.alpakka

import weco.messaging.sqs.SQSConfig
import weco.monitoring.MetricsConfig

case class AlpakkaSQSWorkerConfig(
  metricsConfig: MetricsConfig,
  sqsConfig: SQSConfig
)
