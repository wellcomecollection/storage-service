package uk.ac.wellcome.platform.archive.common.config.builders

import com.typesafe.config.Config
import uk.ac.wellcome.messaging.sns.SNSConfig
import uk.ac.wellcome.messaging.typesafe.SNSBuilder
import uk.ac.wellcome.platform.archive.common.operation.services.OutgoingPublisher

object OutgoingPublisherBuilder {
  def build(config: Config, operationName: String): OutgoingPublisher[SNSConfig] =
    new OutgoingPublisher(
      messageSender = SNSBuilder.buildSNSMessageSender(
        config = config,
        namespace = "outgoing",
        subject = s"Sent from OutgoingPublisher: $operationName"
      )
    )
}
