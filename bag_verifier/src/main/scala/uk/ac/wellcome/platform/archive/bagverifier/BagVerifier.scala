package uk.ac.wellcome.platform.archive.bagverifier

import akka.Done
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.sns.AmazonSNS
import com.amazonaws.services.sns.model.{PublishRequest, PublishResult}
import grizzled.slf4j.Logging
import io.circe.Encoder
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.{NotificationMessage, SNSConfig}
import uk.ac.wellcome.messaging.sqs.SQSStream
import uk.ac.wellcome.platform.archive.bagverifier.config.BagVerifierConfig
import uk.ac.wellcome.platform.archive.common.models.BagRequest
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class BagVerifier(
  s3Client: AmazonS3,
  snsClient: AmazonSNS,
  sqsStream: SQSStream[NotificationMessage],
  bagVerifierConfig: BagVerifierConfig,
  ingestsSnsConfig: SNSConfig,
  outgoingSnsConfig: SNSConfig
)(implicit ec: ExecutionContext)
    extends Logging
    with Runnable {

  def run(): Future[Done] =
    sqsStream.foreach(
      this.getClass.getSimpleName,
      processMessage
    )

  def processMessage(
    notificationMessage: NotificationMessage
  ): Future[Unit] =
    for {
      replicationRequest <- Future.fromTry(
        fromJson[BagRequest](notificationMessage.body)
      )

      _ <- Future.fromTry(
        notifyNext(replicationRequest)
      )
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
