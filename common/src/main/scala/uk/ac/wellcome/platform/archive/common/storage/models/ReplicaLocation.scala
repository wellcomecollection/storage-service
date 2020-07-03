package uk.ac.wellcome.platform.archive.common.storage.models

import uk.ac.wellcome.platform.archive.common.ingests.models.AmazonS3StorageProvider
import uk.ac.wellcome.storage.{Location, Prefix, S3ObjectLocationPrefix}

sealed trait ReplicaLocation {
  val prefix: Prefix[_ <: Location]

  // TODO: Bridging code while we split ObjectLocation.  Remove this later.
  // See https://github.com/wellcomecollection/platform/issues/4596
  def toStorageLocation: StorageLocation
}

object ReplicaLocation {
  // TODO: Bridging code while we split ObjectLocation.  Remove this later.
  // See https://github.com/wellcomecollection/platform/issues/4596
  def fromStorageLocation(storageLocation: StorageLocation): ReplicaLocation =
    storageLocation match {
      case primary: PrimaryStorageLocation =>
        PrimaryS3ReplicaLocation(
          prefix = S3ObjectLocationPrefix(primary.prefix)
        )

      case secondary: SecondaryStorageLocation =>
        SecondaryS3ReplicaLocation(
          prefix = S3ObjectLocationPrefix(secondary.prefix)
        )
    }
}

sealed trait S3ReplicaLocation extends ReplicaLocation {
  val prefix: S3ObjectLocationPrefix
}

sealed trait PrimaryReplicaLocation extends ReplicaLocation {
  override def toStorageLocation: PrimaryStorageLocation
}

sealed trait SecondaryReplicaLocation extends ReplicaLocation {
  override def toStorageLocation: SecondaryStorageLocation
}

case class PrimaryS3ReplicaLocation(
  prefix: S3ObjectLocationPrefix
) extends S3ReplicaLocation
    with PrimaryReplicaLocation {
  override def toStorageLocation: PrimaryStorageLocation =
    PrimaryStorageLocation(
      provider = AmazonS3StorageProvider,
      prefix = prefix.toObjectLocationPrefix
    )
}

case class SecondaryS3ReplicaLocation(
  prefix: S3ObjectLocationPrefix
) extends S3ReplicaLocation
    with SecondaryReplicaLocation {
  override def toStorageLocation: SecondaryStorageLocation =
    SecondaryStorageLocation(
      provider = AmazonS3StorageProvider,
      prefix = prefix.toObjectLocationPrefix
    )
}
