package weco.storage_service.bag_replicator.replicator

import java.time.Instant

import grizzled.slf4j.Logging
import weco.storage_service.bag_replicator.replicator.models._
import weco.storage_service.bagit.models.{BagVersion, ExternalIdentifier}
import weco.storage_service.ingests.models.IngestID
import weco.storage_service.storage.models.{EnsureTrailingSlash, StorageSpace}
import weco.storage_service.storage.services.DestinationBuilder
import weco.storage._
import weco.storage.s3.S3ObjectLocationPrefix
import weco.storage.transfer.{PrefixTransfer, PrefixTransferFailure}

// This is a generic replication from one location to another.
//
// You can extend from this trait to add context specific checks
// after a replication is complete.
//
// For example, in the BagReplicator, we verify the tag manifests
// are the same after replication completes.

trait Replicator[SrcLocation, DstLocation <: Location, DstPrefix <: Prefix[
  DstLocation
]] extends Logging {
  implicit val prefixTransfer: PrefixTransfer[
    S3ObjectLocationPrefix,
    SrcLocation,
    DstPrefix,
    DstLocation
  ]

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
        s"dst = ${request.dstPrefix}"
    )

    prefixTransfer.transferPrefix(
      srcPrefix = replicaSrcPrefix,
      dstPrefix = request.dstPrefix,

      // We used to condition this on whether there were any existing objects in
      // the prefix; if there weren't, we skip checking to avoid getting eventual
      // consistency from S3.  We no longer need to do so.
      //
      // See discussion on https://github.com/wellcomecollection/platform/issues/3897
      checkForExisting = true
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
