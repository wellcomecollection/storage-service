package uk.ac.wellcome.platform.archive.common.config.builders

import com.typesafe.config.Config
import uk.ac.wellcome.messaging.sns.SNSConfig
import uk.ac.wellcome.messaging.typesafe.SNSBuilder
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater

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
