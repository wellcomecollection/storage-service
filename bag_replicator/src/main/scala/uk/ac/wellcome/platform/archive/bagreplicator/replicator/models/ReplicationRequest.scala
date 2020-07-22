package uk.ac.wellcome.platform.archive.bagreplicator.replicator.models

import uk.ac.wellcome.platform.archive.bagreplicator.models.{
  PrimaryReplica,
  ReplicaType,
  SecondaryReplica
}
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  AmazonS3StorageProvider,
  AzureBlobStorageProvider,
  StorageProvider
}
import uk.ac.wellcome.platform.archive.common.storage.models.{
  PrimaryS3ReplicaLocation,
  ReplicaLocation,
  SecondaryAzureReplicaLocation,
  SecondaryS3ReplicaLocation
}
import uk.ac.wellcome.storage.{
  AzureBlobItemLocationPrefix,
  ObjectLocationPrefix,
  S3ObjectLocationPrefix
}

case class ReplicationRequest(
  srcPrefix: S3ObjectLocationPrefix,
  dstPrefix: ObjectLocationPrefix
) {
  def toReplicaLocation(
    provider: StorageProvider,
    replicaType: ReplicaType
  ): ReplicaLocation =
    (provider, replicaType) match {
      case (AmazonS3StorageProvider, PrimaryReplica) =>
        PrimaryS3ReplicaLocation(
          prefix = S3ObjectLocationPrefix(dstPrefix)
        )

      case (AmazonS3StorageProvider, SecondaryReplica) =>
        SecondaryS3ReplicaLocation(
          prefix = S3ObjectLocationPrefix(dstPrefix)
        )

      case (AzureBlobStorageProvider, SecondaryReplica) =>
        SecondaryAzureReplicaLocation(
          prefix = AzureBlobItemLocationPrefix(dstPrefix)
        )

      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported provider/replica type: provider=$provider, replica type=$replicaType"
        )
    }
}
