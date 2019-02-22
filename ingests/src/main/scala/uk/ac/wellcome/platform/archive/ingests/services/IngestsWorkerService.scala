package uk.ac.wellcome.platform.archive.ingests.services

import akka.Done
import uk.ac.wellcome.messaging.sqs.NotificationStream
import uk.ac.wellcome.platform.archive.common.progress.models.ProgressUpdate
import uk.ac.wellcome.platform.archive.common.progress.monitor.ProgressTracker
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class IngestsWorkerService(
  notificationStream: NotificationStream[ProgressUpdate],
  progressTracker: ProgressTracker,
  callbackNotificationService: CallbackNotificationService
)(implicit ec: ExecutionContext) extends Runnable {

  def run(): Future[Done] =
    notificationStream.run(processMessage)

  def processMessage(progressUpdate: ProgressUpdate): Future[Unit] =
    for {
      progress <- Future.fromTry(progressTracker.update(progressUpdate))
      _ <- callbackNotificationService.sendNotification(progress)
    } yield ()
}
