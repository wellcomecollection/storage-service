package uk.ac.wellcome.platform.archive.common.config.builders

import com.typesafe.config.Config
import uk.ac.wellcome.messaging.sns.SNSConfig
import uk.ac.wellcome.messaging.typesafe.SNSBuilder
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater

import scala.concurrent.ExecutionContext

object IngestUpdaterBuilder {
  def build(config: Config, operationName: String)(
    implicit ec: ExecutionContext): IngestUpdater[SNSConfig] =
    new IngestUpdater(
      stepName = operationName,
      messageSender = SNSBuilder.buildSNSMessageSender(
        config = config,
        namespace = "ingest",
        subject = "Sent by IngestUpdater"
      )
    )
}
