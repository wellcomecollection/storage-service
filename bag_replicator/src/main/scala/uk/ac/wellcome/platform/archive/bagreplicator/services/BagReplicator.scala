package uk.ac.wellcome.platform.archive.bagreplicator.services

import java.time.Instant

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bagreplicator.models.ReplicationSummary
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestFailed,
  IngestStepResult,
  IngestStepSucceeded,
  StorageSpace
}
import uk.ac.wellcome.storage.transfer.PrefixTransfer
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

import scala.util.{Success, Try}

class BagReplicator(
  implicit
  prefixTransfer: PrefixTransfer[ObjectLocationPrefix, ObjectLocation]
) extends Logging {

  def replicate(
    srcPrefix: ObjectLocationPrefix,
    space: StorageSpace,
    dstPrefix: ObjectLocationPrefix
  ): Try[IngestStepResult[ReplicationSummary]] = {
    val replicationSummary = ReplicationSummary(
      startTime = Instant.now(),
      srcPrefix = srcPrefix,
      dstPrefix = dstPrefix,
      space = space
    )

    // TODO: Plumb the LocationPrefix type back up through destination
    val copyResult =
      prefixTransfer.transferPrefix(
        srcPrefix = srcPrefix,
        dstPrefix = dstPrefix
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
          )
        )
    }
  }
}
