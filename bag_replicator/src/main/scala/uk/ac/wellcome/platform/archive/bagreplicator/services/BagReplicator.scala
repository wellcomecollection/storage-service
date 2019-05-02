package uk.ac.wellcome.platform.archive.bagreplicator.services

import java.time.Instant

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.bagreplicator.config.ReplicatorDestinationConfig
import uk.ac.wellcome.platform.archive.bagreplicator.models.ReplicationSummary
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestFailed,
  IngestStepResult,
  IngestStepSucceeded,
  StorageSpace
}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.s3.S3PrefixCopier

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class BagReplicator(config: ReplicatorDestinationConfig)(
  implicit s3Client: AmazonS3,
  ec: ExecutionContext) {
  val s3PrefixCopier = S3PrefixCopier(s3Client)

  val destinationBuilder = new DestinationBuilder(
    namespace = config.namespace,
    rootPath = config.rootPath
  )

  def replicate(bagRootLocation: ObjectLocation,
                storageSpace: StorageSpace,
                externalIdentifier: ExternalIdentifier,
                version: Int): Future[IngestStepResult[ReplicationSummary]] = {

    val destination = destinationBuilder.buildDestination(
      storageSpace = storageSpace,
      externalIdentifier = externalIdentifier,
      version = version
    )

    val replicationSummary = ReplicationSummary(
      startTime = Instant.now(),
      bagRootLocation = bagRootLocation,
      storageSpace = storageSpace,
      destination = destination
    )

    val copyResult =
      s3PrefixCopier
        .copyObjects(
          srcLocationPrefix = bagRootLocation,
          dstLocationPrefix = destination
        )

    copyResult.transform {
      case Success(_) =>
        Success(
          IngestStepSucceeded(
            replicationSummary.complete
          )
        )

      case Failure(e) =>
        Success(
          IngestFailed(
            replicationSummary.complete,
            e
          ))
    }
  }
}
