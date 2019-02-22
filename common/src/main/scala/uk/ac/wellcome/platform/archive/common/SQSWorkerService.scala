package uk.ac.wellcome.platform.archive.common

import akka.Done
import io.circe.Decoder
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.NotificationMessage
import uk.ac.wellcome.messaging.sqs.SQSStream
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

abstract class SQSWorkerService[T](sqsStream: SQSStream[NotificationMessage])(implicit decoder: Decoder[T], ec: ExecutionContext)
    extends Runnable {
  def run(): Future[Done] =
    sqsStream.foreach(this.getClass.getSimpleName, processNotification)

  def processNotification(notificationMessage: NotificationMessage): Future[Unit] =
    for {
      message <- Future.fromTry(fromJson[T](notificationMessage.body))
      _ <- processMessage(message)
    } yield ()

  def processMessage(message: T): Future[Unit]
}
