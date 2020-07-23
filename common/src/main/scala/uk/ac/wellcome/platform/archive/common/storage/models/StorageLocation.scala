package uk.ac.wellcome.platform.archive.common.storage.models

import uk.ac.wellcome.storage.{AzureBlobItemLocationPrefix, Location, Prefix, S3ObjectLocationPrefix}

sealed trait StorageLocation {
  val prefix: Prefix[_ <: Location]
}

sealed trait PrimaryStorageLocation extends StorageLocation
sealed trait SecondaryStorageLocation extends StorageLocation

case class PrimaryS3StorageLocation(
  prefix: S3ObjectLocationPrefix
) extends PrimaryStorageLocation

case class SecondaryS3StorageLocation(
  prefix: S3ObjectLocationPrefix
) extends SecondaryStorageLocation

case class SecondaryAzureStorageLocation(
  prefix: AzureBlobItemLocationPrefix
) extends SecondaryStorageLocation

case object StorageLocation {
  def createPrimary(prefix: Prefix[_ <: Location]): PrimaryStorageLocation =
    prefix match {
      case s3Prefix: S3ObjectLocationPrefix => PrimaryS3StorageLocation(s3Prefix)
      case _ => throw new IllegalArgumentException(
        s"Unrecognised prefix for primary location: $prefix"
      )
    }

  def createSecondary(prefix: Prefix[_ <: Location]): SecondaryStorageLocation =
    prefix match {
      case s3Prefix: S3ObjectLocationPrefix         => SecondaryS3StorageLocation(s3Prefix)
      case azurePrefix: AzureBlobItemLocationPrefix => SecondaryAzureStorageLocation(azurePrefix)
      case _ => throw new IllegalArgumentException(
        s"Unrecognised prefix for primary location: $prefix"
      )
    }
}
