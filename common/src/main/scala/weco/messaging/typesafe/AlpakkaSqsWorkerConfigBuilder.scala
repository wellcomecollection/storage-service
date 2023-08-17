package weco.messaging.typesafe

import com.typesafe.config.Config
import weco.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import weco.monitoring.typesafe.MetricsBuilder

object AlpakkaSqsWorkerConfigBuilder {
  def build(config: Config) =
    AlpakkaSQSWorkerConfig(
      metricsConfig = MetricsBuilder.buildMetricsConfig(config),
      sqsConfig = SQSBuilder.buildSQSConfig(config)
    )
}
