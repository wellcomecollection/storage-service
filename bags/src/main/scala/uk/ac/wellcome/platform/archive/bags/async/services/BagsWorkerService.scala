package uk.ac.wellcome.platform.archive.bags.async.services

import java.util.UUID

import akka.Done
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.NotificationMessage
import uk.ac.wellcome.messaging.sqs.SQSStream
import uk.ac.wellcome.platform.archive.bags.async.models.BagManifestUpdate
import uk.ac.wellcome.platform.archive.bags.common.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.models.ReplicationResult
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class BagsWorkerService(
  sqsStream: SQSStream[NotificationMessage],
  storageManifestService: StorageManifestService,
  updateStoredManifestService: UpdateStoredManifestService
)(implicit ec: ExecutionContext) extends Runnable {

  def run(): Future[Done] =
    sqsStream.foreach(this.getClass.getSimpleName, processMessage)

  def processMessage(notificationMessage: NotificationMessage): Future[Unit] =
    for {
      replicationResult <- Future.fromTry(
        fromJson[ReplicationResult](notificationMessage.body)
      )
      bagManifestUpdate = BagManifestUpdate(
        archiveRequestId = replicationResult.archiveRequestId,
        archiveBagLocation = replicationResult.srcBagLocation,
        accessBagLocation = replicationResult.dstBagLocation
      )
      tryManifest <- storageManifestService.createManifest(bagManifestUpdate)
      _ <- updateManifest(replicationResult.archiveRequestId, tryManifest)
    } yield ()

  private def updateManifest(archiveRequestId: UUID, tryManifest: Either[Throwable, StorageManifest]): Future[Unit] =
    tryManifest match {
      case Right(storageManifest) =>
        updateStoredManifestService.updateManifest(
          archiveRequestId = archiveRequestId,
          storageManifest = storageManifest
        )
      case Left(_) => Future.successful(())
    }
}
