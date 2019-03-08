package uk.ac.wellcome.platform.archive.bagreplicator.services

import java.time.Instant

import uk.ac.wellcome.platform.archive.bagreplicator.config.ReplicatorDestinationConfig
import uk.ac.wellcome.platform.archive.bagreplicator.models.ReplicationSummary
import uk.ac.wellcome.platform.archive.common.ingests.operation.{
  OperationFailure,
  OperationResult,
  OperationSuccess
}
import uk.ac.wellcome.platform.archive.common.models.bagit.{
  BagLocation,
  BagPath,
  ExternalIdentifier
}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.s3.S3PrefixCopier

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class BagReplicator(
  bagLocator: BagLocator,
  config: ReplicatorDestinationConfig,
  s3PrefixCopier: S3PrefixCopier
) {
  def replicate(
    location: BagLocation
  )(implicit ec: ExecutionContext)
    : Future[OperationResult[ReplicationSummary]] = {

    val replicationSummary = ReplicationSummary(
      startTime = Instant.now(),
      source = location
    )

    val copyOperation = for {
      identifier <- bagLocator.getBagIdentifier(
        location.objectLocation
      )

      destination = buildDestination(
        location,
        identifier
      )

      bagRoot <- bagLocator.getBagRoot(
        location.objectLocation
      )

      _ <- copyBag(
        bagRoot,
        destination
      )

    } yield destination

    copyOperation.transform {
      case Success(location) =>
        Success(
          OperationSuccess(
            replicationSummary
              .copy(destination = Some(location))
              .complete))

      case Failure(e) =>
        Success(
          OperationFailure(
            replicationSummary.complete,
            e
          ))
    }
  }

  private def copyBag(
    bagRoot: ObjectLocation,
    destination: BagLocation
  ) =
    s3PrefixCopier
      .copyObjects(
        srcLocationPrefix = bagRoot,
        dstLocationPrefix = destination.objectLocation
      )

  private def buildDestination(
    location: BagLocation,
    id: ExternalIdentifier
  ) = BagLocation(
    storageNamespace = config.namespace,
    storagePrefix = config.rootPath,
    storageSpace = location.storageSpace,
    bagPath = BagPath(id.underlying)
  )

}
