package uk.ac.wellcome.platform.storage.bagreplicator.services

import akka.Done
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.{PublishAttempt, SNSWriter}
import uk.ac.wellcome.messaging.sqs.NotificationStream
import uk.ac.wellcome.platform.archive.common.bagit.S3BagFile
import uk.ac.wellcome.platform.archive.common.models.bagit.BagLocation
import uk.ac.wellcome.platform.archive.common.models.{BagRequest, ReplicationResult}
import uk.ac.wellcome.platform.archive.common.progress.models._
import uk.ac.wellcome.platform.storage.bagreplicator.config.BagReplicatorConfig
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class BagReplicatorWorkerService(
  notificationStream: NotificationStream[BagRequest],
  bagStorageService: BagStorageService,
  bagReplicatorConfig: BagReplicatorConfig,
  s3BagFile: S3BagFile,
  progressSnsWriter: SNSWriter,
  outgoingSnsWriter: SNSWriter)(implicit ec: ExecutionContext)
    extends Runnable {

  def run(): Future[Done] =
    notificationStream.run(processMessage)

  def processMessage(bagRequest: BagRequest): Future[Unit] =
    for {
      bagInfoPath: Try[String] <- Future {
        s3BagFile.locateBagInfo(bagRequest.bagLocation.objectLocation)
      }
      _ = println(s"@@AWLC bagInfoPath = $bagInfoPath")
      result: Either[Throwable, BagLocation] <- bagStorageService.duplicateBag(
        sourceBagLocation = bagRequest.bagLocation,
        destinationConfig = bagReplicatorConfig.destination
      )
      _ <- sendProgressUpdate(
        bagRequest = bagRequest,
        result = result
      )
      _ <- sendOutgoingNotification(
        bagRequest = bagRequest,
        result = result
      )
    } yield ()

  def sendOutgoingNotification(
    bagRequest: BagRequest,
    result: Either[Throwable, BagLocation]): Future[Unit] =
    result match {
      case Left(_) => Future.successful(())
      case Right(dstBagLocation) =>
        val result = ReplicationResult(
          archiveRequestId = bagRequest.archiveRequestId,
          srcBagLocation = bagRequest.bagLocation,
          dstBagLocation = dstBagLocation
        )
        outgoingSnsWriter
          .writeMessage(
            result,
            subject = s"Sent by ${this.getClass.getSimpleName}"
          )
          .map { _ =>
            ()
          }
    }

  def sendProgressUpdate(
    bagRequest: BagRequest,
    result: Either[Throwable, BagLocation]): Future[PublishAttempt] = {
    val event: ProgressUpdate = result match {
      case Right(_) =>
        ProgressUpdate.event(
          id = bagRequest.archiveRequestId,
          description = "Bag replicated successfully"
        )
      case Left(_) =>
        ProgressStatusUpdate(
          id = bagRequest.archiveRequestId,
          status = Progress.Failed,
          affectedBag = None,
          events = List(ProgressEvent("Failed to replicate bag"))
        )
    }

    progressSnsWriter.writeMessage(
      event,
      subject = s"Sent by ${this.getClass.getSimpleName}"
    )
  }
}
