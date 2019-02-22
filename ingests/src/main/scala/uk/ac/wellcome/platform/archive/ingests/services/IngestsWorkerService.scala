package uk.ac.wellcome.platform.archive.ingests.services

import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.NotificationMessage
import uk.ac.wellcome.messaging.sqs.SQSStream
import uk.ac.wellcome.platform.archive.common.SQSWorkerService
import uk.ac.wellcome.platform.archive.common.progress.models.ProgressUpdate
import uk.ac.wellcome.platform.archive.common.progress.monitor.ProgressTracker

import scala.concurrent.{ExecutionContext, Future}

class IngestsWorkerService(
  sqsStream: SQSStream[NotificationMessage],
  progressTracker: ProgressTracker,
  callbackNotificationService: CallbackNotificationService
)(implicit ec: ExecutionContext) extends SQSWorkerService(sqsStream) {

  def processMessage(notificationMessage: NotificationMessage): Future[Unit] =
    for {
      progressUpdate <- Future.fromTry(
        fromJson[ProgressUpdate](notificationMessage.body)
      )
      progress <- Future.fromTry(progressTracker.update(progressUpdate))
      _ <- callbackNotificationService.sendNotification(progress)
    } yield ()
}
