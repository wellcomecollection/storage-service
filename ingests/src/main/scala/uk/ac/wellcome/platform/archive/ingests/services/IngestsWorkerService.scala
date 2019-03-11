package uk.ac.wellcome.platform.archive.ingests.services

import akka.Done
import uk.ac.wellcome.messaging.sqs.NotificationStream
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestUpdate
import uk.ac.wellcome.platform.archive.common.ingests.monitor.IngestTracker
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class IngestsWorkerService(
  notificationStream: NotificationStream[IngestUpdate],
  ingestTracker: IngestTracker,
  callbackNotificationService: CallbackNotificationService
)(implicit ec: ExecutionContext)
    extends Runnable {

  def run(): Future[Done] =
    notificationStream.run(processMessage)

  def processMessage(ingestUpdate: IngestUpdate): Future[Unit] =
    for {
      ingest <- Future.fromTry(
        ingestTracker.update(ingestUpdate)
      )
      _ <- callbackNotificationService
        .sendNotification(ingest)
    } yield ()
}
