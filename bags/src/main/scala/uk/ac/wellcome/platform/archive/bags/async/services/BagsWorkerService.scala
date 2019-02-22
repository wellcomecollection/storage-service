package uk.ac.wellcome.platform.archive.bags.async.services

import java.util.UUID

import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.{NotificationMessage, PublishAttempt, SNSWriter}
import uk.ac.wellcome.messaging.sqs.SQSStream
import uk.ac.wellcome.platform.archive.bags.async.models.BagManifestUpdate
import uk.ac.wellcome.platform.archive.bags.common.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.SQSWorkerService
import uk.ac.wellcome.platform.archive.common.models.ReplicationResult
import uk.ac.wellcome.platform.archive.common.progress.models.{Progress, ProgressEvent, ProgressStatusUpdate, ProgressUpdate}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class BagsWorkerService(
  sqsStream: SQSStream[NotificationMessage],
  storageManifestService: StorageManifestService,
  updateStoredManifestService: UpdateStoredManifestService,
  progressSnsWriter: SNSWriter)(implicit ec: ExecutionContext)
  extends SQSWorkerService(sqsStream)
  with Logging {

  def processMessage(notificationMessage: NotificationMessage): Future[Unit] =
    for {
      replicationResult <- Future.fromTry(
        fromJson[ReplicationResult](notificationMessage.body)
      )
      bagManifestUpdate =
        BagManifestUpdate(
          archiveRequestId = replicationResult.archiveRequestId,
          archiveBagLocation = replicationResult.srcBagLocation,
          accessBagLocation = replicationResult.dstBagLocation
        )
      tryStorageManifest: Try[StorageManifest] <- storageManifestService.createManifest(bagManifestUpdate)
      tryUpdateVHSResult: Try[Unit] <- updateStorageManifest(tryStorageManifest)
      _ <- sendProgressUpdate(
        archiveRequestId = replicationResult.archiveRequestId,
        tryStorageManifest = tryStorageManifest,
        tryUpdateVHSResult = tryUpdateVHSResult
      )
    } yield ()

  private def updateStorageManifest(tryStorageManifest: Try[StorageManifest]): Future[Try[Unit]] =
    tryStorageManifest match {
      case Success(storageManifest) => updateStoredManifestService.updateStoredManifest(storageManifest)
      case Failure(err) => Future.successful(Failure(err))
    }

  private def sendProgressUpdate(
    archiveRequestId: UUID,
    tryStorageManifest: Try[StorageManifest],
    tryUpdateVHSResult: Try[Unit]): Future[PublishAttempt] = {
    val event = (tryStorageManifest, tryUpdateVHSResult) match {
      case (Success(storageManifest), Success(_)) =>
        ProgressStatusUpdate(
          id = archiveRequestId,
          status = Progress.Completed,
          affectedBag = Some(storageManifest.id),
          events = List(ProgressEvent("Bag registered successfully"))
        )
      case (Success(storageManifest), Failure(_)) =>
        ProgressStatusUpdate(
          id = archiveRequestId,
          status = Progress.Failed,
          affectedBag = Some(storageManifest.id),
          events = List(ProgressEvent("Failed to register bag"))
        )
      case _ =>
        ProgressStatusUpdate(
          id = archiveRequestId,
          status = Progress.Failed,
          affectedBag = None,
          events = List(ProgressEvent("Failed to create storage manifest"))
        )
    }

    progressSnsWriter.writeMessage[ProgressUpdate](
      event,
      subject = s"Sent from ${this.getClass.getSimpleName}"
    )
  }
}
