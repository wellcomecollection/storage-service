package uk.ac.wellcome.platform.archive.notifier.services

import akka.Done
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.SNSWriter
import uk.ac.wellcome.platform.archive.common.messaging.NotificationStream
import uk.ac.wellcome.platform.archive.common.models.CallbackNotification
import uk.ac.wellcome.platform.archive.common.progress.models.ProgressUpdate
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class NotifierWorkerService(notificationStream: NotificationStream[CallbackNotification],
                            callbackUrlService: CallbackUrlService,
                            snsWriter: SNSWriter)(implicit ec: ExecutionContext)
    extends Runnable {

  def run(): Future[Done] =
    notificationStream.run(processMessage)

  def processMessage(callbackNotification: CallbackNotification): Future[Unit] =
    for {
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
