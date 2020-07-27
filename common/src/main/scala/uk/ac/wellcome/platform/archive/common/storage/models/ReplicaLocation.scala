package uk.ac.wellcome.platform.archive.common.storage.models

import uk.ac.wellcome.storage.azure.AzureBlobLocationPrefix
import uk.ac.wellcome.storage.s3.S3ObjectLocationPrefix
import uk.ac.wellcome.storage.{Location, Prefix}

sealed trait ReplicaLocation {
  val prefix: Prefix[_ <: Location]
}

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
  prefix: AzureBlobLocationPrefix
) extends SecondaryReplicaLocation
