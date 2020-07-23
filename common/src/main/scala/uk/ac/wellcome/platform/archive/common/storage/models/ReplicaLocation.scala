package uk.ac.wellcome.platform.archive.common.storage.models

import uk.ac.wellcome.storage.{AzureBlobItemLocationPrefix, Location, Prefix, S3ObjectLocationPrefix}

sealed trait ReplicaLocation {
  val prefix: Prefix[_ <: Location]
}

object ReplicaLocation

sealed trait S3ReplicaLocation extends ReplicaLocation {
  val prefix: S3ObjectLocationPrefix
}

sealed trait PrimaryReplicaLocation extends ReplicaLocation
sealed trait SecondaryReplicaLocation extends ReplicaLocation

case class PrimaryS3ReplicaLocation(
  prefix: S3ObjectLocationPrefix
) extends S3ReplicaLocation
    with PrimaryReplicaLocation

case class SecondaryS3ReplicaLocation(
  prefix: S3ObjectLocationPrefix
) extends S3ReplicaLocation
    with SecondaryReplicaLocation

case class SecondaryAzureReplicaLocation(
  prefix: AzureBlobItemLocationPrefix
) extends SecondaryReplicaLocation
