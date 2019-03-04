package uk.ac.wellcome.platform.storage.bagreplicator.services

import akka.Done
import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.{PublishAttempt, SNSWriter}
import uk.ac.wellcome.messaging.sqs.NotificationStream
import uk.ac.wellcome.platform.archive.common.ConvertibleToInputStream._
import uk.ac.wellcome.platform.archive.common.bagit.S3BagFile
import uk.ac.wellcome.platform.archive.common.models.bagit.{BagLocation, BagPath, ExternalIdentifier}
import uk.ac.wellcome.platform.archive.common.models.{BagRequest, ReplicationResult}
import uk.ac.wellcome.platform.archive.common.parsers.BagInfoParser
import uk.ac.wellcome.platform.archive.common.progress.models._
import uk.ac.wellcome.platform.storage.bagreplicator.config.BagReplicatorConfig
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class BagReplicatorWorkerService(
  notificationStream: NotificationStream[BagRequest],
  bagStorageService: BagStorageService,
  bagReplicatorConfig: BagReplicatorConfig,
  s3BagFile: S3BagFile,
  progressSnsWriter: SNSWriter,
  outgoingSnsWriter: SNSWriter)(implicit ec: ExecutionContext, s3Client: AmazonS3)
    extends Runnable {

  def run(): Future[Done] =
    notificationStream.run(processMessage)

  def processMessage(bagRequest: BagRequest): Future[Unit] =
    for {
      tryBagInfoPath: Try[String] <- Future {
        s3BagFile.locateBagInfo(bagRequest.bagLocation.objectLocation)
      }
      _ = println(s"@@AWLC bagInfoPath = $tryBagInfoPath")
      tryExternalIdentifier <- getBagExternalIdentifier(bagRequest, tryBagInfoPath)
      _ = println(s"@@AWLC externalIdentifier = $tryExternalIdentifier")
      dstBagLocation = createDstBagLocation(
        bagRequest, dstNamespace = "archivez", tryExternalIdentifier = tryExternalIdentifier
      )
      _ = println(s"@@AWLC dstBagLocation = $dstBagLocation")


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

    private def createDstBagLocation(bagRequest: BagRequest, dstNamespace: String, tryExternalIdentifier: Try[ExternalIdentifier]): Try[BagLocation] =
      tryExternalIdentifier.map { externalIdentifier =>
        BagLocation(
          storageNamespace = dstNamespace,
          storagePrefix = None,
          storageSpace = bagRequest.bagLocation.storageSpace,
          bagPath = BagPath(externalIdentifier.underlying)
        )
      }

  private def getBagExternalIdentifier(
    bagRequest: BagRequest,
    tryBagInfoPath: Try[String]): Future[Try[ExternalIdentifier]] =
    tryBagInfoPath match {
      case Success(bagInfoPath) =>
        val objectLocation = bagRequest.bagLocation.objectLocation.copy(
          key = bagInfoPath
        )

        val externalIdentifier: Future[ExternalIdentifier] = for {
          inputStream <- objectLocation.toInputStream
          bagInfo <- BagInfoParser.create(inputStream)
          externalIdentifier = bagInfo.externalIdentifier
        } yield externalIdentifier

        externalIdentifier
          .map { Success(_) }
          .recover { case err: Throwable => Failure(err) }
      case Failure(err) => Future.successful(Failure(err))
    }

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
