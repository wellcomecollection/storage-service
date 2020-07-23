package uk.ac.wellcome.platform.archive.common.storage.models

import uk.ac.wellcome.platform.archive.common.ingests.models.{AmazonS3StorageProvider, StorageProvider}
import uk.ac.wellcome.storage._

// Represents the location of all the versions of a given (space, externalIdentifier) pair
sealed trait NewStorageLocation {
  val provider: StorageProvider
  val prefix: Prefix[_ <: Location]
}

sealed trait NewPrimaryStorageLocation
  extends NewStorageLocation

sealed trait NewSecondaryStorageLocation
  extends NewStorageLocation

sealed trait S3StorageLocation extends NewStorageLocation {
  val prefix: S3ObjectLocationPrefix
  val provider: StorageProvider = AmazonS3StorageProvider
}

sealed trait AzureStorageLocation extends NewStorageLocation {
  val prefix: AzureBlobItemLocationPrefix
  val provider: StorageProvider = AmazonS3StorageProvider
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



sealed trait StorageLocation {
  val provider: StorageProvider
  val prefix: ObjectLocationPrefix
}

case class PrimaryStorageLocation(
  provider: StorageProvider,
  prefix: ObjectLocationPrefix
) extends StorageLocation

case class SecondaryStorageLocation(
  provider: StorageProvider,
  prefix: ObjectLocationPrefix
) extends StorageLocation
