package uk.ac.wellcome.platform.archive.bagreplicator.services

import java.io.InputStream
import java.nio.file.Paths
import java.time.Instant

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.bagreplicator.config.ReplicatorDestinationConfig
import uk.ac.wellcome.platform.archive.bagreplicator.models.ReplicationSummary
import uk.ac.wellcome.platform.archive.common.ConvertibleToInputStream._
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagInfo,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.bagit.parsers.BagInfoParser
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestFailed,
  IngestStepResult,
  IngestStepSucceeded,
  StorageSpace
}
import uk.ac.wellcome.platform.archive.common.storage.services.S3BagLocator
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.s3.{S3PrefixCopier, S3PrefixCopierResult}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class BagReplicator(config: ReplicatorDestinationConfig)(
  implicit s3Client: AmazonS3,
  ec: ExecutionContext) {
  val s3PrefixCopier = S3PrefixCopier(s3Client)
  val s3BagLocator = new S3BagLocator(s3Client)

  def replicate(bagRootLocation: ObjectLocation, storageSpace: StorageSpace)
    : Future[IngestStepResult[ReplicationSummary]] = {
    val replicationSummary = ReplicationSummary(
      startTime = Instant.now(),
      bagRootLocation = bagRootLocation,
      storageSpace = storageSpace
    )

    val copyOperation = for {
      identifier <- getBagIdentifier(bagRootLocation)

      destination = buildDestination(
        storageSpace = storageSpace,
        id = identifier
      )

      _ <- copyBag(
        bagRoot = bagRootLocation,
        destination = destination
      )
    } yield destination

    copyOperation.transform {
      case Success(dstLocation) =>
        Success(
          IngestStepSucceeded(
            replicationSummary
              .copy(maybeDestination = Some(dstLocation))
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
    destination: ObjectLocation
  ): Future[S3PrefixCopierResult] =
    s3PrefixCopier
      .copyObjects(
        srcLocationPrefix = bagRoot,
        dstLocationPrefix = destination
      )

  private def buildDestination(
    storageSpace: StorageSpace,
    id: ExternalIdentifier
  ): ObjectLocation = ObjectLocation(
    namespace = config.namespace,
    key = Paths
      .get(
        config.rootPath.getOrElse(""),
        storageSpace.toString,
        id.toString
      )
      .toString
  )

  private def getBagIdentifier(
    bagRootLocation: ObjectLocation): Future[ExternalIdentifier] =
    for {
      bagInfoLocation <- Future.fromTry {
        s3BagLocator.locateBagInfo(bagRootLocation)
      }
      inputStream: InputStream <- bagInfoLocation.toInputStream
      bagInfo: BagInfo <- BagInfoParser.create(inputStream)
    } yield bagInfo.externalIdentifier

}
