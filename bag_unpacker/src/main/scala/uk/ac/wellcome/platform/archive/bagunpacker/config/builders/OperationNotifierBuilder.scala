package uk.ac.wellcome.platform.archive.bagunpacker.config.builders

import com.typesafe.config.Config
import uk.ac.wellcome.messaging.typesafe.SNSBuilder
import uk.ac.wellcome.platform.archive.bagunpacker.services.OperationNotifierService

object OperationNotifierBuilder {
  def build(config: Config, operationName: String): OperationNotifierService = {
    new OperationNotifierService(
      operationName,
      outgoingSnsWriter = SNSBuilder.buildSNSWriter(
        config,
        namespace = "outgoing"
      ),
      progressSnsWriter = SNSBuilder.buildSNSWriter(
        config,
        namespace = "progress"
      )
    )
  }
}
