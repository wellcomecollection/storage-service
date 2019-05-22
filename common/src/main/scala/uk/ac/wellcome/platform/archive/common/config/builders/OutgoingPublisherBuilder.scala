package uk.ac.wellcome.platform.archive.common.config.builders

import com.typesafe.config.Config
import uk.ac.wellcome.messaging.typesafe.SNSBuilder
import uk.ac.wellcome.platform.archive.common.operation.services.OutgoingPublisher

object OutgoingPublisherBuilder {
  def build(config: Config, operationName: String): OutgoingPublisher =
    new OutgoingPublisher(
      operationName = operationName,
      snsWriter = SNSBuilder.buildSNSWriter(
        config = config,
        namespace = "outgoing"
      )
    )
}
