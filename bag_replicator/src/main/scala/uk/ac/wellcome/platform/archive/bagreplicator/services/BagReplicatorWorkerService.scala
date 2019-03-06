package uk.ac.wellcome.platform.archive.bagreplicator.services

import akka.Done
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.{PublishAttempt, SNSWriter}
import uk.ac.wellcome.messaging.sqs.NotificationStream
import uk.ac.wellcome.platform.archive.bagreplicator.config.ReplicatorDestinationConfig
import uk.ac.wellcome.platform.archive.common.models.BagRequest
import uk.ac.wellcome.platform.archive.common.models.bagit.{
  BagLocation,
  BagPath,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.progress.models._
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.s3.S3PrefixCopier
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class BagReplicatorWorkerService(
  notificationStream: NotificationStream[BagRequest],
  unpackedBagService: UnpackedBagService,
  s3PrefixCopier: S3PrefixCopier,
  replicatorDestinationConfig: ReplicatorDestinationConfig,
  progressSnsWriter: SNSWriter,
  outgoingSnsWriter: SNSWriter)(implicit ec: ExecutionContext)
    extends Runnable {

  def run(): Future[Done] =
    notificationStream.run(processMessage)

  def processMessage(bagRequest: BagRequest): Future[Unit] =
    for {
      externalIdentifier <- getExternalIdentifier(bagRequest)
      bagRoot <- getBagRoot(bagRequest)
      dstBagLocation = buildDstBagLocation(bagRequest, externalIdentifier)
      result <- copyBag(bagRoot, dstBagLocation)
      dstBagRequest: Either[Throwable, BagRequest] = dstBagLocation.map {
        bagLocation =>
          BagRequest(
            requestId = bagRequest.requestId,
            bagLocation = bagLocation
          )
      }
      _ <- sendProgressUpdate(
        bagRequest = bagRequest,
        result = result
      )
      _ <- sendOutgoingNotification(
        dstBagRequest = dstBagRequest,
        result = result
      )
    } yield ()

  private def getExternalIdentifier(
    bagRequest: BagRequest): Future[Either[Throwable, ExternalIdentifier]] =
    either {
      unpackedBagService.getBagIdentifier(bagRequest.bagLocation.objectLocation)
    }

  private def getBagRoot(
    bagRequest: BagRequest): Future[Either[Throwable, ObjectLocation]] =
    either {
      unpackedBagService.getBagRoot(bagRequest.bagLocation.objectLocation)
    }

  private def buildDstBagLocation(
    bagRequest: BagRequest,
    maybeExternalIdentifier: Either[Throwable, ExternalIdentifier])
    : Either[Throwable, BagLocation] =
    maybeExternalIdentifier.map { externalIdentifier =>
      BagLocation(
        storageNamespace = replicatorDestinationConfig.namespace,
        storagePrefix = replicatorDestinationConfig.rootPath,
        storageSpace = bagRequest.bagLocation.storageSpace,
        bagPath = BagPath(externalIdentifier.underlying)
      )
    }

  private def copyBag(maybeBagRoot: Either[Throwable, ObjectLocation],
                      maybeDstLocation: Either[Throwable, BagLocation])
    : Future[Either[Throwable, Unit]] =
    (maybeBagRoot, maybeDstLocation) match {
      case (Right(srcLocation), Right(dstLocation)) =>
        s3PrefixCopier
          .copyObjects(
            srcLocationPrefix = srcLocation,
            dstLocationPrefix = dstLocation.objectLocation
          )
          .map { Right(_) }
      case (Left(err), _) => Future.successful(Left(err))
      case (_, Left(err)) => Future.successful(Left(err))
    }

  def either[T](future: Future[T]): Future[Either[Throwable, T]] =
    future
      .map { Right(_) }
      .recover { case err: Throwable => Left(err) }

  def sendOutgoingNotification(dstBagRequest: Either[Throwable, BagRequest],
                               result: Either[Throwable, Unit]): Future[Unit] =
    (dstBagRequest, result) match {
      case (Right(bagRequest), Right(_)) =>
        outgoingSnsWriter
          .writeMessage(
            bagRequest,
            subject = s"Sent by ${this.getClass.getSimpleName}"
          )
          .map { _ =>
            ()
          }
      case _ => Future.successful(())
    }

  def sendProgressUpdate(
    bagRequest: BagRequest,
    result: Either[Throwable, Unit]): Future[PublishAttempt] = {
    val event: ProgressUpdate = result match {
      case Right(_) =>
        ProgressUpdate.event(
          id = bagRequest.requestId,
          description = "Bag successfully copied from ingest location"
        )
      case Left(_) =>
        ProgressStatusUpdate(
          id = bagRequest.requestId,
          status = Progress.Failed,
          affectedBag = None,
          events =
            List(ProgressEvent("Failed to copy bag from ingest location"))
        )
    }

    progressSnsWriter.writeMessage(
      event,
      subject = s"Sent by ${this.getClass.getSimpleName}"
    )
  }
}
