package uk.ac.wellcome.platform.archive.progress_async.services

import akka.Done
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.NotificationMessage
import uk.ac.wellcome.messaging.sqs.SQSStream
import uk.ac.wellcome.platform.archive.common.progress.models.ProgressUpdate
import uk.ac.wellcome.platform.archive.common.progress.monitor.ProgressTracker
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class IngestsWorkerService(
  sqsStream: SQSStream[NotificationMessage],
  progressTracker: ProgressTracker,
  callbackNotificationService: CallbackNotificationService
)(implicit ec: ExecutionContext)
    extends Runnable {
  def run(): Future[Done] =
    sqsStream.foreach(this.getClass.getSimpleName, processMessage)

  def processMessage(notificationMessage: NotificationMessage): Future[Unit] =
    for {
      progressUpdate <- Future.fromTry(
        fromJson[ProgressUpdate](notificationMessage.body)
      )
      progress <- Future.fromTry(progressTracker.update(progressUpdate))
      _ <- callbackNotificationService.sendNotification(progress)
    } yield ()
}
