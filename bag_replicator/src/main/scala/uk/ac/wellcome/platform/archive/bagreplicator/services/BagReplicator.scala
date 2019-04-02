package uk.ac.wellcome.platform.archive.bagreplicator.services

import java.time.Instant

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.bagreplicator.config.ReplicatorDestinationConfig
import uk.ac.wellcome.platform.archive.bagreplicator.models.ReplicationSummary
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagLocation,
  BagPath,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.operation.services.{
  IngestFailed,
  IngestStepResult,
  IngestStepSuccess
}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.s3.S3PrefixCopier

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class BagReplicator(
  bagLocator: BagLocator,
  config: ReplicatorDestinationConfig,
  s3PrefixCopier: S3PrefixCopier
)(implicit s3Client: AmazonS3, ec: ExecutionContext) {
  val s3BagLocator = new S3BagLocator(s3Client)

  def replicate(
    location: BagLocation
  ): Future[IngestStepResult[ReplicationSummary]] = {
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

      bagRoot <- Future.fromTry {
        s3BagLocator.locateBagRoot(location.objectLocation)
      }

      _ <- copyBag(
        bagRoot,
        destination
      )

    } yield destination

    copyOperation.transform {
      case Success(dstLocation) =>
        Success(
          IngestStepSuccess(
            replicationSummary
              .copy(destination = Some(dstLocation))
              .complete))

      case Failure(e) =>
        Success(
          IngestFailed(
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
