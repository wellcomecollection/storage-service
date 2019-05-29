package uk.ac.wellcome.platform.archive.bagreplicator.services

import java.time.Instant

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bagreplicator.models.ReplicationSummary
import uk.ac.wellcome.platform.archive.common.storage.models.{IngestFailed, IngestStepResult, IngestStepSucceeded, StorageSpace}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.s3.S3PrefixCopier

import scala.util.{Success, Try}

class BagReplicator(implicit s3Client: AmazonS3) extends Logging {
  val s3PrefixCopier = S3PrefixCopier(s3Client)

  def replicate(bagRootLocation: ObjectLocation,
                storageSpace: StorageSpace,
                destination: ObjectLocation): Try[IngestStepResult[ReplicationSummary]] = {
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

    copyResult match {
      case Right(_) =>
        Success(
          IngestStepSucceeded(
            replicationSummary.complete
          )
        )

      case Left(storageError) =>
        error("Unexpected failure while replicating", storageError.e)
        Success(
          IngestFailed(
            replicationSummary.complete,
            storageError.e
          ))
    }
  }
}
