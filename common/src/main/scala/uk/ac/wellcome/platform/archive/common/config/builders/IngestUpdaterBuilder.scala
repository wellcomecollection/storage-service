package uk.ac.wellcome.platform.archive.common.config.builders

import com.typesafe.config.Config
import uk.ac.wellcome.messaging.typesafe.SNSBuilder
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater

object IngestUpdaterBuilder {
  def build(config: Config, operationName: String): IngestUpdater =
    new IngestUpdater(
      stepName = operationName,
      snsWriter = SNSBuilder.buildSNSWriter(
        config = config,
        namespace = "ingest"
      )
    )
}
