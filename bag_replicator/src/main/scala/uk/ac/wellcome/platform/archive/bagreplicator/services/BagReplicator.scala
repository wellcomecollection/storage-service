package uk.ac.wellcome.platform.archive.bagreplicator.services

import java.time.Instant

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bagreplicator.models.ReplicationSummary
import uk.ac.wellcome.platform.archive.common.BagReplicaLocation
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestFailed,
  IngestStepResult,
  IngestStepSucceeded,
  StorageSpace
}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.transfer.s3.S3PrefixTransfer

import scala.util.{Success, Try}

class BagReplicator(implicit s3Client: AmazonS3) extends Logging {
  val s3PrefixTransfer = S3PrefixTransfer()

  def replicate(
    bagRootLocation: ObjectLocation,
    storageSpace: StorageSpace,
    destination: BagReplicaLocation): Try[IngestStepResult[ReplicationSummary]] = {
    val replicationSummary = ReplicationSummary(
      startTime = Instant.now(),
      bagRootLocation = bagRootLocation,
      storageSpace = storageSpace,
      destination = destination
    )

    val copyResult =
      s3PrefixTransfer.transferPrefix(
        srcPrefix = bagRootLocation.asPrefix,
        dstPrefix = destination.asPrefix
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
