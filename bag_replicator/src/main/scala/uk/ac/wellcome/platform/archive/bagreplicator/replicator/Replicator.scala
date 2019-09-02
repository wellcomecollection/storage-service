package uk.ac.wellcome.platform.archive.bagreplicator.replicator

import java.time.Instant

import uk.ac.wellcome.platform.archive.bagreplicator.replicator.models._
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.storage.transfer.{PrefixTransfer, TransferResult}
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

// This is a generic replication from one location to another.
//
// You can extend from this trait to add context specific checks
// after a replication is complete.
//
// For example, in the BagReplicator, we verify the tag manifests
// are the same after replication completes.

trait Replicator {
  implicit val prefixTransfer: PrefixTransfer[
    ObjectLocationPrefix,
    ObjectLocation
  ]

  def replicate(ingestId: IngestID, request: ReplicationRequest): ReplicationResult = {
    val summary = ReplicationSummary(
      ingestId = ingestId,
      startTime = Instant.now,
      request = request
    )

    prefixTransfer.transferPrefix(
      srcPrefix = request.srcPrefix,
      dstPrefix = request.dstPrefix
    ) match {
      case Right(_) =>
        ReplicationSucceeded(summary.complete)

      case Left(err: TransferResult) =>
        ReplicationFailed(summary.complete, err.e)
    }
  }
}
