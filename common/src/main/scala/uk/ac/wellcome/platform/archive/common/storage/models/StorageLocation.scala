package uk.ac.wellcome.platform.archive.common.storage.models

import uk.ac.wellcome.platform.archive.common.ingests.models.{
  AmazonS3StorageProvider,
  AzureBlobStorageProvider,
  StorageProvider
}
import uk.ac.wellcome.storage._

// Represents the location of all the versions of a given (space, externalIdentifier) pair
sealed trait StorageLocation {
  val provider: StorageProvider
  val prefix: Prefix[_ <: Location]
}

sealed trait PrimaryStorageLocation extends StorageLocation

case object PrimaryStorageLocation {
  def apply(prefix: Prefix[_ <: Location]): PrimaryStorageLocation =
    prefix match {
      case s3Prefix: S3ObjectLocationPrefix =>
        PrimaryS3StorageLocation(s3Prefix)

      case _ =>
        throw new IllegalArgumentException(s"Unrecognised location: $prefix")
    }
}

sealed trait SecondaryStorageLocation extends StorageLocation

case object SecondaryStorageLocation {
  def apply(prefix: Prefix[_ <: Location]): SecondaryStorageLocation =
    prefix match {
      case s3Prefix: S3ObjectLocationPrefix =>
        SecondaryS3StorageLocation(s3Prefix)

      case azurePrefix: AzureBlobItemLocationPrefix =>
        SecondaryAzureStorageLocation(azurePrefix)

      case _ =>
        throw new IllegalArgumentException(s"Unrecognised location: $prefix")
    }
}

sealed trait S3StorageLocation extends StorageLocation {
  val prefix: S3ObjectLocationPrefix
  val provider: StorageProvider = AmazonS3StorageProvider
}

sealed trait AzureStorageLocation extends StorageLocation {
  val prefix: AzureBlobItemLocationPrefix
  val provider: StorageProvider = AzureBlobStorageProvider
}

case class PrimaryS3StorageLocation(prefix: S3ObjectLocationPrefix)
    extends PrimaryStorageLocation
    with S3StorageLocation

case class SecondaryS3StorageLocation(prefix: S3ObjectLocationPrefix)
    extends SecondaryStorageLocation
    with S3StorageLocation

case class SecondaryAzureStorageLocation(prefix: AzureBlobItemLocationPrefix)
    extends SecondaryStorageLocation
    with AzureStorageLocation
