package weco.storage_service.config.builders

import com.typesafe.config.Config
import weco.messaging.sns.SNSConfig
import weco.messaging.typesafe.SNSBuilder
import weco.storage_service.operation.services.OutgoingPublisher

object OutgoingPublisherBuilder {
  def build(
    config: Config,
    operationName: String
  ): OutgoingPublisher[SNSConfig] =
    new OutgoingPublisher(
      messageSender = SNSBuilder.buildSNSMessageSender(
        config = config,
        namespace = "outgoing",
        subject = s"Sent from OutgoingPublisher: $operationName"
      )
    )
}
