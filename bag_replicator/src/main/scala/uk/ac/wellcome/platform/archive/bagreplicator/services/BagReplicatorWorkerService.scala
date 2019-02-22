package uk.ac.wellcome.platform.archive.bagreplicator.services

import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.{
  NotificationMessage,
  PublishAttempt,
  SNSWriter
}
import uk.ac.wellcome.messaging.sqs.SQSStream
import uk.ac.wellcome.platform.archive.bagreplicator.config.BagReplicatorConfig
import uk.ac.wellcome.platform.archive.common.SQSWorkerService
import uk.ac.wellcome.platform.archive.common.models.bagit.BagLocation
import uk.ac.wellcome.platform.archive.common.models.{
  BagRequest,
  ReplicationResult
}
import uk.ac.wellcome.platform.archive.common.progress.models._

import scala.concurrent.{ExecutionContext, Future}

class BagReplicatorWorkerService(
  sqsStream: SQSStream[NotificationMessage],
  bagStorageService: BagStorageService,
  bagReplicatorConfig: BagReplicatorConfig,
  progressSnsWriter: SNSWriter,
  outgoingSnsWriter: SNSWriter)(implicit ec: ExecutionContext)
    extends SQSWorkerService(sqsStream)
    with Logging {

  def processMessage(notificationMessage: NotificationMessage): Future[Unit] =
    for {
      replicationRequest <- Future.fromTry(
        fromJson[BagRequest](notificationMessage.body)
      )
      result: Either[Throwable, BagLocation] <- bagStorageService.duplicateBag(
        sourceBagLocation = replicationRequest.bagLocation,
        destinationConfig = bagReplicatorConfig.destination
      )
      _ <- sendProgressUpdate(
        replicationRequest = replicationRequest,
        result = result
      )
      _ <- sendOngoingNotification(
        replicationRequest = replicationRequest,
        result = result
      )
    } yield ()

  def sendOngoingNotification(
    replicationRequest: BagRequest,
    result: Either[Throwable, BagLocation]): Future[Unit] =
    result match {
      case Left(_) => Future.successful(())
      case Right(dstBagLocation) =>
        val result = ReplicationResult(
          archiveRequestId = replicationRequest.archiveRequestId,
          srcBagLocation = replicationRequest.bagLocation,
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
    replicationRequest: BagRequest,
    result: Either[Throwable, BagLocation]): Future[PublishAttempt] = {
    val event: ProgressUpdate = result match {
      case Right(_) =>
        ProgressUpdate.event(
          id = replicationRequest.archiveRequestId,
          description = "Bag replicated successfully"
        )
      case Left(_) =>
        ProgressStatusUpdate(
          id = replicationRequest.archiveRequestId,
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
