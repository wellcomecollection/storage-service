package uk.ac.wellcome.platform.archive.bagreplicator.replicator

import java.time.Instant

import uk.ac.wellcome.platform.archive.bagreplicator.models._
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.models._
import uk.ac.wellcome.storage.transfer.{PrefixTransfer, TransferResult}
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

import scala.concurrent.Future

// This is a generic replication from one location to another.
//
// You can extend from this trait to add context specific checks
// after a replication is complete.
//
// For example, in the BagReplicator, we verify the tag manifests
// are the same after replication completes.

trait Replicator {
  implicit val prefixTransfer: PrefixTransfer[ObjectLocationPrefix, ObjectLocation]

  def replicate(replication: ReplicationRequest): Future[ReplicationResult] = {
    val summary = ReplicationSummary(
      startTime = Instant.now,
      request = replication
    )

    prefixTransfer.transferPrefix(
      srcPrefix = replication.srcPrefix,
      dstPrefix = replication.dstPrefix
    ) map {
      case Right(_) =>
        ReplicationSucceeded(summary.complete)

      case Left(err: TransferResult) =>
        ReplicationFailed(summary.complete, err.e)

    } recover {
      case err =>
        ReplicationFailed(summary.complete, err)
    }
  }
}
