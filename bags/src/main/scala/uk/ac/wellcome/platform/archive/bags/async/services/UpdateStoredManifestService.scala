package uk.ac.wellcome.platform.archive.bags.async.services

import java.util.UUID

import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.{PublishAttempt, SNSWriter}
import uk.ac.wellcome.platform.archive.bags.common.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.models.bagit.BagId
import uk.ac.wellcome.platform.archive.common.progress.models.{Progress, ProgressEvent, ProgressStatusUpdate, ProgressUpdate}
import uk.ac.wellcome.storage.ObjectStore
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.vhs.{EmptyMetadata, VersionedHybridStore}

import scala.concurrent.{ExecutionContext, Future}

class UpdateStoredManifestService(
  vhs: VersionedHybridStore[StorageManifest,
                            EmptyMetadata,
                            ObjectStore[StorageManifest]],
  progressSnsWriter: SNSWriter)(implicit ec: ExecutionContext) {

  def updateManifest(archiveRequestId: UUID, storageManifest: StorageManifest): Future[Unit] =
    for {
      result <- updateVHS(storageManifest)
      _ <- sendProgressUpdate(
        requestId = archiveRequestId,
        bagId = storageManifest.id,
        result = result
      )
    } yield ()

  private def sendProgressUpdate(requestId: UUID, bagId: BagId, result: Either[Throwable, Unit]): Future[PublishAttempt] = {
    val event: ProgressUpdate = result match {
      case Right(_) =>
        ProgressUpdate.event(
          id = requestId,
          description = "Bag registered successfully"
        )
      case Left(_) =>
        ProgressStatusUpdate(
          id = requestId,
          status = Progress.Failed,
          affectedBag = Some(bagId),
          events = List(ProgressEvent("Failed to register bag"))
        )
    }

    progressSnsWriter.writeMessage(
      event,
      subject = s"Sent by ${this.getClass.getSimpleName}"
    )
  }

  private def updateVHS(storageManifest: StorageManifest): Future[Either[Throwable, Unit]] =
    vhs.updateRecord(storageManifest.id.toString)(
      ifNotExisting = (storageManifest, EmptyMetadata()))(
      ifExisting = (_, _) => (storageManifest, EmptyMetadata())
    )
      .map { _ => Right(()) }
      .recover { case err: Throwable => Left(err)}
}
