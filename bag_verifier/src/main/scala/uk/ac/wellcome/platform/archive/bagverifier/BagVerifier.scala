package uk.ac.wellcome.platform.archive.bagverifier

import akka.Done
import akka.actor.ActorSystem
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.sns.AmazonSNS
import com.amazonaws.services.sns.model.{PublishRequest, PublishResult}
import grizzled.slf4j.Logging
import io.circe.Encoder
import uk.ac.wellcome.messaging.sns.SNSConfig
import uk.ac.wellcome.messaging.sqs.NotificationStream
import uk.ac.wellcome.platform.archive.bagverifier.config.BagVerifierConfig
import uk.ac.wellcome.typesafe.Runnable
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.models.BagRequest

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

class BagVerifier(
  s3Client: AmazonS3,
  snsClient: AmazonSNS,
  notificationStream: NotificationStream[BagRequest],
  bagVerifierConfig: BagVerifierConfig,
  ingestsSnsConfig: SNSConfig,
  outgoingSnsConfig: SNSConfig
)(
  implicit val actorSystem: ActorSystem
) extends Logging
    with Runnable {

  def run(): Future[Done] =
    notificationStream.run(processMessage)

  def processMessage(bagRequest: BagRequest): Future[Unit] =
    for {
      _ <- Future.fromTry(notifyNext(bagRequest))
    } yield ()

  private def notifyNext(
    bagRequest: BagRequest
  ): Try[PublishResult] =
    publishNotification[BagRequest](
      bagRequest,
      outgoingSnsConfig
    )

  private def publishNotification[T](
    msg: T,
    snsConfig: SNSConfig
  )(
    implicit encoder: Encoder[T]
  ): Try[PublishResult] = {
    toJson[T](msg)
      .map { messageString =>
        debug(s"snsPublishMessage: $messageString")

        new PublishRequest(
          snsConfig.topicArn,
          messageString,
          "bag_verifier"
        )
      }
      .map(snsClient.publish)
  }
}
