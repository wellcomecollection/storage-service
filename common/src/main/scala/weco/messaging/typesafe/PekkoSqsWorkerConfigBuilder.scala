package weco.messaging.typesafe

import com.typesafe.config.Config
import weco.messaging.sqsworker.pekko.PekkoSQSWorkerConfig
import weco.monitoring.typesafe.MetricsBuilder

object PekkoSQSWorkerConfigBuilder {
  def build(config: Config) =
    PekkoSQSWorkerConfig(
      metricsConfig = MetricsBuilder.buildMetricsConfig(config),
      sqsConfig = SQSBuilder.buildSQSConfig(config)
    )
}
