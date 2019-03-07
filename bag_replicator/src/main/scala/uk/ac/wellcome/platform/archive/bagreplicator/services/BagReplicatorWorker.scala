package uk.ac.wellcome.platform.archive.bagreplicator.services

import akka.Done
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.SNSWriter
import uk.ac.wellcome.messaging.sqs.NotificationStream
import uk.ac.wellcome.platform.archive.bagreplicator.config.ReplicatorDestinationConfig
import uk.ac.wellcome.platform.archive.common.models.BagRequest
import uk.ac.wellcome.platform.archive.common.models.bagit.{
  BagLocation,
  BagPath,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.operation.{
  OperationFailure,
  OperationNotifier,
  OperationResult,
  OperationSuccess
}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.s3.S3PrefixCopier
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class BagReplicatorWorker(
  notificationStream: NotificationStream[BagRequest],
  bagLocator: BagLocator,
  s3PrefixCopier: S3PrefixCopier,
  replicatorDestinationConfig: ReplicatorDestinationConfig,
  progressSnsWriter: SNSWriter,
  outgoingSnsWriter: SNSWriter)(implicit ec: ExecutionContext)
    extends Runnable {

  val operationNotifier = new OperationNotifier(
    operationName = "Copy bag from ingest bucket",
    outgoingSnsWriter = outgoingSnsWriter,
    progressSnsWriter = progressSnsWriter
  )

  def run(): Future[Done] =
    notificationStream.run(processMessage)

  def processMessage(bagRequest: BagRequest): Future[Unit] =
    for {
      externalIdentifier <- getExternalIdentifier(bagRequest)
      bagRoot <- getBagRoot(bagRequest)
      dstBagLocation = buildDstBagLocation(bagRequest, externalIdentifier)
      copyResult: Either[Throwable, BagLocation] <- copyBag(
        bagRoot,
        dstBagLocation)
      operationResult: OperationResult[BagLocation] = copyResult match {
        case Right(bagLocation) => OperationSuccess(bagLocation)
        case Left(throwable) =>
          OperationFailure(bagRequest.bagLocation, throwable)
      }
      _ <- operationNotifier.send(
        requestId = bagRequest.requestId,
        result = operationResult
      ) { bagLocation =>
        BagRequest(
          requestId = bagRequest.requestId,
          bagLocation = bagLocation
        )
      }
    } yield ()

  private def getExternalIdentifier(
    bagRequest: BagRequest): Future[Either[Throwable, ExternalIdentifier]] =
    either {
      bagLocator.getBagIdentifier(bagRequest.bagLocation.objectLocation)
    }

  private def getBagRoot(
    bagRequest: BagRequest): Future[Either[Throwable, ObjectLocation]] =
    either {
      bagLocator.getBagRoot(bagRequest.bagLocation.objectLocation)
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
    : Future[Either[Throwable, BagLocation]] =
    (maybeBagRoot, maybeDstLocation) match {
      case (Right(srcLocation), Right(dstLocation)) =>
        s3PrefixCopier
          .copyObjects(
            srcLocationPrefix = srcLocation,
            dstLocationPrefix = dstLocation.objectLocation
          )
          .map { _ =>
            Right(dstLocation)
          }
      case (Left(err), _) => Future.successful(Left(err))
      case (_, Left(err)) => Future.successful(Left(err))
    }

  def either[T](future: Future[T]): Future[Either[Throwable, T]] =
    future
      .map { Right(_) }
      .recover { case err: Throwable => Left(err) }
}
