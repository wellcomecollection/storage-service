package weco.storage_service.config.builders

import com.typesafe.config.Config
import weco.messaging.sns.SNSConfig
import weco.messaging.typesafe.SNSBuilder
import weco.storage_service.ingests.services.IngestUpdater

object IngestUpdaterBuilder {
  def build(config: Config, operationName: String): IngestUpdater[SNSConfig] =
    new IngestUpdater(
      stepName = operationName,
      messageSender = SNSBuilder.buildSNSMessageSender(
        config = config,
        namespace = "ingest",
        subject = s"Sent from IngestUpdater: $operationName"
      )
    )
}
