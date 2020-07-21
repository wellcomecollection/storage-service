package uk.ac.wellcome.platform.archive.bagreplicator.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.bagreplicator.replicator.models.ReplicationRequest
import uk.ac.wellcome.platform.archive.common.ingests.models.StorageProvider
import uk.ac.wellcome.platform.archive.common.storage.models.{
  PrimaryStorageLocation,
  ReplicaResult,
  SecondaryStorageLocation
}

// For bag replicas, we distinguish between primary and secondary replicas.
//
//  - The primary replica is the "warm" copy, intended for frequent access
//    e.g. an S3 bucket with Standard-IA
//
//  - Secondary replicas are the "cold" copies, that give us an extra layer
//    of backup but aren't intended for frequent access
//    e.g. an S3 Glacier bucket

sealed trait BagReplicationRequest {
  val request: ReplicationRequest

  override def toString: String =
    this match {
      case PrimaryBagReplicationRequest(req) =>
        f"""replica=primary src=${req.srcPrefix} dst=${req.dstPrefix}"""

      case SecondaryBagReplicationRequest(req) =>
        f"""replica=secondary src=${req.srcPrefix} dst=${req.dstPrefix}"""
    }

  def toResult(provider: StorageProvider): ReplicaResult = {
    val storageLocation =
      this match {
        case PrimaryBagReplicationRequest(req) =>
          PrimaryStorageLocation(
            provider = provider,
            prefix = this.request.dstPrefix
          )

        case SecondaryBagReplicationRequest(req) =>
          SecondaryStorageLocation(
            provider = provider,
            prefix = this.request.dstPrefix
          )
      }

    ReplicaResult(
      originalLocation = request.srcPrefix,
      storageLocation = storageLocation,
      timestamp = Instant.now()
    )
  }
}

case class PrimaryBagReplicationRequest(request: ReplicationRequest)
    extends BagReplicationRequest

case class SecondaryBagReplicationRequest(request: ReplicationRequest)
    extends BagReplicationRequest
