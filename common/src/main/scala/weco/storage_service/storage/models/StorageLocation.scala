package weco.storage_service.storage.models

import weco.storage_service.ingests.models.{
  AmazonS3StorageProvider,
  AzureBlobStorageProvider,
  StorageProvider
}
import weco.storage.{Location, Prefix}
import weco.storage.providers.azure.AzureBlobLocationPrefix
import weco.storage.providers.s3.S3ObjectLocationPrefix

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

      case azurePrefix: AzureBlobLocationPrefix =>
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
  val prefix: AzureBlobLocationPrefix
  val provider: StorageProvider = AzureBlobStorageProvider
}

case class PrimaryS3StorageLocation(prefix: S3ObjectLocationPrefix)
    extends PrimaryStorageLocation
    with S3StorageLocation

case class SecondaryS3StorageLocation(prefix: S3ObjectLocationPrefix)
    extends SecondaryStorageLocation
    with S3StorageLocation

case class SecondaryAzureStorageLocation(prefix: AzureBlobLocationPrefix)
    extends SecondaryStorageLocation
    with AzureStorageLocation
