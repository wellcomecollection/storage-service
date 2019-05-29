package uk.ac.wellcome.platform.archive.bagreplicator.services

import java.time.Instant

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.bagreplicator.models.ReplicationSummary
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestFailed,
  IngestStepResult,
  IngestStepSucceeded,
  StorageSpace
}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.s3.S3PrefixCopier

import scala.util.{Failure, Success, Try}

class BagReplicator(implicit s3Client: AmazonS3) {
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
