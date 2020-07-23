package uk.ac.wellcome.platform.archive.common.storage.models

import uk.ac.wellcome.platform.archive.common.ingests.models.{AmazonS3StorageProvider, AzureBlobStorageProvider, StorageProvider}
import uk.ac.wellcome.storage._

// Represents the location of all the versions of a given (space, externalIdentifier) pair
sealed trait NewStorageLocation {
  val provider: StorageProvider
  val prefix: Prefix[_ <: Location]
}

sealed trait NewPrimaryStorageLocation
  extends NewStorageLocation

case object NewPrimaryStorageLocation {
  def apply(prefix: Prefix[_ <: Location]): NewPrimaryStorageLocation =
    prefix match {
      case s3Prefix: S3ObjectLocationPrefix =>
        PrimaryS3StorageLocation(s3Prefix)

      case _ => throw new IllegalArgumentException(s"Unrecognised location: $prefix")
    }
}

sealed trait NewSecondaryStorageLocation
  extends NewStorageLocation

case object NewSecondaryStorageLocation {
  def apply(prefix: Prefix[_ <: Location]): NewSecondaryStorageLocation =
    prefix match {
      case s3Prefix: S3ObjectLocationPrefix =>
        SecondaryS3StorageLocation(s3Prefix)

      case azurePrefix: AzureBlobItemLocationPrefix =>
        SecondaryAzureStorageLocation(azurePrefix)

      case _ => throw new IllegalArgumentException(s"Unrecognised location: $prefix")
    }
}

sealed trait S3StorageLocation extends NewStorageLocation {
  val prefix: S3ObjectLocationPrefix
  val provider: StorageProvider = AmazonS3StorageProvider
}

sealed trait AzureStorageLocation extends NewStorageLocation {
  val prefix: AzureBlobItemLocationPrefix
  val provider: StorageProvider = AzureBlobStorageProvider
}

case class PrimaryS3StorageLocation(prefix: S3ObjectLocationPrefix)
  extends NewPrimaryStorageLocation
    with S3StorageLocation

case class SecondaryS3StorageLocation(prefix: S3ObjectLocationPrefix)
  extends NewSecondaryStorageLocation
    with S3StorageLocation

case class SecondaryAzureStorageLocation(prefix: AzureBlobItemLocationPrefix)
  extends NewSecondaryStorageLocation
    with AzureStorageLocation
