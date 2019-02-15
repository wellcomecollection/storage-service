package uk.ac.wellcome.platform.archive.notifier.services

import akka.Done
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.{NotificationMessage, SNSWriter}
import uk.ac.wellcome.messaging.sqs.SQSStream
import uk.ac.wellcome.platform.archive.common.models.CallbackNotification
import uk.ac.wellcome.platform.archive.common.progress.models.ProgressUpdate
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class NotifierWorkerService(callbackUrlService: CallbackUrlService,
                            sqsStream: SQSStream[NotificationMessage],
                            snsWriter: SNSWriter)(implicit ec: ExecutionContext)
    extends Runnable {

  def run(): Future[Done] =
    sqsStream.foreach(this.getClass.getSimpleName, processMessage)

  def processMessage(notificationMessage: NotificationMessage): Future[Unit] =
    for {
      callbackNotification <- Future.fromTry(
        fromJson[CallbackNotification](notificationMessage.body)
      )
      httpResponse <- callbackUrlService.getHttpResponse(callbackNotification)
      progressUpdate = PrepareNotificationService.prepare(
        callbackNotification.id,
        httpResponse)
      _ <- snsWriter.writeMessage[ProgressUpdate](
        progressUpdate,
        subject = s"Sent by ${this.getClass.getName}"
      )
    } yield ()
}
