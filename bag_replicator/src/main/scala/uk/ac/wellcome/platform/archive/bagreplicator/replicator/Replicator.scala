package uk.ac.wellcome.platform.archive.bagreplicator.replicator

import java.time.Instant

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.models._
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.storage.listing.Listing
import uk.ac.wellcome.storage.transfer.{PrefixTransfer, TransferResult}
import uk.ac.wellcome.storage.{
  ObjectLocation,
  ObjectLocationPrefix,
  S3ObjectLocation,
  S3ObjectLocationPrefix
}

// This is a generic replication from one location to another.
//
// You can extend from this trait to add context specific checks
// after a replication is complete.
//
// For example, in the BagReplicator, we verify the tag manifests
// are the same after replication completes.

trait Replicator extends Logging {
  implicit val prefixTransfer: PrefixTransfer[
    ObjectLocationPrefix,
    ObjectLocation
  ]

  implicit val prefixListing: Listing[
    S3ObjectLocationPrefix,
    S3ObjectLocation
  ]

  def replicate(
    ingestId: IngestID,
    request: ReplicationRequest
  ): ReplicationResult = {
    val summary = ReplicationSummary(
      ingestId = ingestId,
      startTime = Instant.now,
      request = request
    )

    // If we know that there is nothing under the prefix (and we're the only
    // replicator writing to it due to the lock), we can skip checking for
    // overwrites.
    //
    // This gives us read-after-write consistency, rather than eventual
    // consistency, and makes the verifier more reliable.
    //
    // See: https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html
    //
    //      Amazon S3 provides read-after-write consistency for PUTS of new objects
    //      in your S3 bucket in all Regions with one caveat. The caveat is that if
    //      you make a HEAD or GET request to a key name before the object is created,
    //      then create the object shortly after that, a subsequent GET might not
    //      return the object due to eventual consistency.
    //
    // See also: https://github.com/wellcomecollection/platform/issues/3897
    //
    // In theory this could be even more sophisticated, and compare the keys to
    // replicate against a list of existing keys -- but that would make the code
    // even more complicated, and it's not clear it would be beneficial.
    //
    val checkForExisting = prefixListing.list(request.srcPrefix) match {
      case Right(listing) if listing.isEmpty => false
      case _                                 => true
    }

    debug(s"Consistency mode: checkForExisting = $checkForExisting")

    prefixTransfer.transferPrefix(
      srcPrefix = request.srcPrefix.toObjectLocationPrefix,
      dstPrefix = request.dstPrefix,
      checkForExisting = checkForExisting
    ) match {
      case Right(_) =>
        ReplicationSucceeded(summary.complete)

      case Left(err: TransferResult) =>
        ReplicationFailed(summary.complete, err.e)
    }
  }
}
