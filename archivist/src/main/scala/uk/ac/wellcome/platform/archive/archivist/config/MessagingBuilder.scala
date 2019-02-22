package uk.ac.wellcome.platform.archive.archivist.config

import akka.actor.ActorSystem
import com.typesafe.config.Config
import uk.ac.wellcome.messaging.typesafe.SQSBuilder
import uk.ac.wellcome.monitoring.typesafe.MetricsSenderBuilder
import uk.ac.wellcome.platform.archive.archivist.messaging.MessageStream

object MessagingBuilder {
  def buildMessageStream[T, R](config: Config)(
    implicit actorSystem: ActorSystem): MessageStream[T, R] =
    new MessageStream[T, R](
      sqsClient = SQSBuilder.buildSQSAsyncClient(config),
      sqsConfig = SQSBuilder.buildSQSConfig(config),
      metricsSender = MetricsSenderBuilder.buildMetricsSender(config)
    )
}
