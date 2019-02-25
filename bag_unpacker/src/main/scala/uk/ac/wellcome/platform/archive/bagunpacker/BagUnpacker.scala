package uk.ac.wellcome.platform.archive.bagunpacker

import akka.Done
import akka.actor.ActorSystem
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.sns.AmazonSNS
import com.amazonaws.services.sns.model.{PublishRequest, PublishResult}
import grizzled.slf4j.Logging
import io.circe.Encoder
import uk.ac.wellcome.messaging.sns.SNSConfig
import uk.ac.wellcome.messaging.sqs.NotificationStream
import uk.ac.wellcome.platform.archive.bagunpacker.config.BagUnpackerConfig
import uk.ac.wellcome.typesafe.Runnable
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.models.{
  BagRequest,
  UnpackBagRequest
}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

class BagUnpacker(
  notificationStream: NotificationStream[UnpackBagRequest],
  s3Client: AmazonS3,
  snsClient: AmazonSNS,
  bagUnpackerConfig: BagUnpackerConfig,
  ingestsSnsConfig: SNSConfig,
  outgoingSnsConfig: SNSConfig
)(
  implicit val actorSystem: ActorSystem
) extends Logging
    with Runnable {

  def run(): Future[Done] =
    notificationStream.run(processMessage)

  def processMessage(unpackBagRequest: UnpackBagRequest): Future[Unit] =
    for {
      _ <- Future.fromTry(
        notifyNext(
          BagRequest(
            archiveRequestId = unpackBagRequest.requestId,
            bagLocation = unpackBagRequest.bagDestination
          )
        )
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
          "bag_unpacker"
        )
      }
      .map(snsClient.publish)
  }
}
