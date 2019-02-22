package uk.ac.wellcome.platform.archive.common.messaging

import akka.actor.ActorSystem
import com.typesafe.config.Config
import io.circe.Decoder
import uk.ac.wellcome.messaging.sns.NotificationMessage
import uk.ac.wellcome.messaging.typesafe.SQSBuilder

import scala.concurrent.ExecutionContext

object NotificationStreamBuilder {
  def buildStream[T](config: Config)(implicit decoder: Decoder[T], actorSystem: ActorSystem, ec: ExecutionContext): NotificationStream[T] =
    new NotificationStream[T](
      sqsStream = SQSBuilder.buildSQSStream[NotificationMessage](config)
    )
}
