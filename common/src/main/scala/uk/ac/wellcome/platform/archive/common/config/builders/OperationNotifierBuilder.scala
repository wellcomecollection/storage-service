package uk.ac.wellcome.platform.archive.common.config.builders

import com.typesafe.config.Config
import uk.ac.wellcome.messaging.typesafe.SNSBuilder
import uk.ac.wellcome.platform.archive.common.ingests.operation.OperationNotifier

object OperationNotifierBuilder {
  def build(config: Config, operationName: String): OperationNotifier = {
    new OperationNotifier(
      operationName,
      outgoingSnsWriter = SNSBuilder.buildSNSWriter(
        config,
        namespace = "outgoing"
      ),
      ingestSnsWriter = SNSBuilder.buildSNSWriter(
        config,
        namespace = "ingest"
      )
    )
  }
}
