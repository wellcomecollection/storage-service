package uk.ac.wellcome.platform.archive.bagunpacker.services

import java.nio.file.Paths
import java.util.UUID

import akka.Done
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.{PublishAttempt, SNSWriter}
import uk.ac.wellcome.messaging.sqs.NotificationStream
import uk.ac.wellcome.platform.archive.bagunpacker.BagUnpackerConfig
import uk.ac.wellcome.platform.archive.common.models.bagit.{BagLocation, BagPath}
import uk.ac.wellcome.platform.archive.common.models.{BagRequest, UnpackBagRequest}
import uk.ac.wellcome.platform.archive.common.progress.models.{Progress, ProgressEvent, ProgressStatusUpdate, ProgressUpdate}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class BagUnpackerWorkerService(
  config: BagUnpackerConfig,
  stream: NotificationStream[UnpackBagRequest],
  progressSnsWriter: SNSWriter,
  outgoingSnsWriter: SNSWriter,
  unpackerService: UnpackerService
) extends Logging
    with Runnable {

  def run(): Future[Done] =
    stream.run(processMessage)

  def processMessage(
    unpackBagRequest: UnpackBagRequest
  ): Future[Unit] = {

    val notificationService = new NotificationService(outgoingSnsWriter, progressSnsWriter)

    val destinationLocation = UnpackDestination(
      config.namespace,
      config.prefix,
      unpackBagRequest.requestId
    )

    for {
      unpackResult <- unpackerService.unpack(
        unpackBagRequest.sourceLocation, destinationLocation
      )

      _ = info(s"Unpacked bag: $unpackResult")

      _ <- notificationService.sendOutgoingNotification(unpackBagRequest)
    } yield unpackResult
  }
}

class NotificationService(outgoingSnsWriter: SNSWriter, progressSnsWriter: SNSWriter) {
  def sendOutgoingNotification(unpackBagRequest: UnpackBagRequest) = {
    outgoingSnsWriter
      .writeMessage(
        BagRequest(
          unpackBagRequest.requestId,

          // TODO: This is wrong!
          BagLocation(
            storageNamespace = "uploadNamespace",
            storagePrefix = Some("uploadPrefix"),
            unpackBagRequest.storageSpace,
            bagPath = BagPath("externalIdentifier")
          )
        ),
        subject = s"Sent by ${this.getClass.getSimpleName}"
      )
      .map(_ => ())
  }

  def sendProgressNotification(
                          bagRequest: BagRequest,
                          result: Either[Throwable, BagLocation]): Future[PublishAttempt] = {
    val event: ProgressUpdate = result match {
      case Right(_) =>
        ProgressUpdate.event(
          id = bagRequest.archiveRequestId,
          description = "Bag unpacked successfully"
        )
      case Left(_) =>
        ProgressStatusUpdate(
          id = bagRequest.archiveRequestId,
          status = Progress.Failed,
          affectedBag = None,
          events = List(ProgressEvent("Failed to unpack bag"))
        )
    }

    progressSnsWriter.writeMessage(
      event,
      subject = s"Sent by ${this.getClass.getSimpleName}"
    )
  }
}

object UnpackDestination {
  def apply(namespace: String, prefix: String, id: UUID) = {
    ObjectLocation(
      namespace = namespace,
      key = Paths.get(prefix, id.toString).toString
    )
  }
}