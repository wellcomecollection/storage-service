package uk.ac.wellcome.platform.archive.bagreplicator.replicator

import java.time.Instant

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.models._
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagVersion,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models.{
  EnsureTrailingSlash,
  StorageSpace
}
import uk.ac.wellcome.platform.archive.common.storage.services.DestinationBuilder
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.listing.Listing
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import uk.ac.wellcome.storage.transfer.{PrefixTransfer, PrefixTransferFailure}

// This is a generic replication from one location to another.
//
// You can extend from this trait to add context specific checks
// after a replication is complete.
//
// For example, in the BagReplicator, we verify the tag manifests
// are the same after replication completes.

trait Replicator[DstLocation <: Location, DstPrefix <: Prefix[DstLocation]]
    extends Logging {
  implicit val prefixTransfer: PrefixTransfer[
    S3ObjectLocationPrefix,
    S3ObjectLocation,
    DstPrefix,
    DstLocation
  ]

  implicit val dstListing: Listing[DstPrefix, DstLocation]

  def buildDestination(
    namespace: String,
    space: StorageSpace,
    externalIdentifier: ExternalIdentifier,
    version: BagVersion
  ): DstPrefix =
    buildDestinationFromParts(
      namespace = namespace,
      path = DestinationBuilder.buildPath(space, externalIdentifier, version)
    )

  protected def buildDestinationFromParts(
    namespace: String,
    path: String
  ): DstPrefix

  def replicate(
    ingestId: IngestID,
    request: ReplicationRequest[DstPrefix]
  ): ReplicationResult[DstPrefix] = {
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
    val checkForExisting = dstListing.list(request.dstPrefix) match {
      case Right(listing) if listing.isEmpty => false
      case _                                 => true
    }

    // It's important that we add slashes to the end of our prefixes, so we're
    // listing a "directory" in S3, and we don't get partial "directories".
    //
    // e.g. if asked to transfer  "s3://bukkit/bags/v1", we shouldn't also
    // transfer entries in prefix "s3://bukkit/bags/v10"
    //
    // See https://github.com/wellcomecollection/platform/issues/4745
    import EnsureTrailingSlash._
    val replicaSrcPrefix = request.srcPrefix.withTrailingSlash

    debug(
      "Triggering PrefixTransfer: " + "" +
        s"src = $replicaSrcPrefix, " +
        s"dst = ${request.dstPrefix}, " +
        s"checkForExisting = $checkForExisting"
    )

    prefixTransfer.transferPrefix(
      srcPrefix = replicaSrcPrefix,
      dstPrefix = request.dstPrefix,
      checkForExisting = checkForExisting
    ) match {
      case Right(_) =>
        ReplicationSucceeded(summary.complete)

      case Left(err: PrefixTransferFailure) =>
        ReplicationFailed(
          summary.complete,
          e = new Throwable(s"Prefix transfer failed: $err")
        )
    }
  }
}
