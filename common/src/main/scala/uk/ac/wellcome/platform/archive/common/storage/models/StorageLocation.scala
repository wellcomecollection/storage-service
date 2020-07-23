package uk.ac.wellcome.platform.archive.common.storage.models

import uk.ac.wellcome.platform.archive.common.ingests.models.{
  AmazonS3StorageProvider,
  AzureBlobStorageProvider,
  StorageProvider
}
import uk.ac.wellcome.storage.{
  AzureBlobItemLocationPrefix,
  Location,
  Prefix,
  S3ObjectLocationPrefix
}

sealed trait StorageLocation {
  val prefix: Prefix[_ <: Location]
  val provider: StorageProvider
}

sealed trait PrimaryStorageLocation extends StorageLocation
sealed trait SecondaryStorageLocation extends StorageLocation

sealed trait S3StorageLocation extends StorageLocation {
  val prefix: S3ObjectLocationPrefix
  override val provider: StorageProvider = AmazonS3StorageProvider
}

sealed trait AzureStorageLocation extends StorageLocation {
  val prefix: AzureBlobItemLocationPrefix
  override val provider: StorageProvider = AzureBlobStorageProvider
}

case class PrimaryS3StorageLocation(
  prefix: S3ObjectLocationPrefix
) extends PrimaryStorageLocation
    with S3StorageLocation

case class SecondaryS3StorageLocation(
  prefix: S3ObjectLocationPrefix
) extends SecondaryStorageLocation
    with S3StorageLocation

case class SecondaryAzureStorageLocation(
  prefix: AzureBlobItemLocationPrefix
) extends SecondaryStorageLocation
    with AzureStorageLocation

case object StorageLocation {
  def createPrimary(prefix: Prefix[_ <: Location]): PrimaryStorageLocation =
    prefix match {
      case s3Prefix: S3ObjectLocationPrefix =>
        PrimaryS3StorageLocation(s3Prefix)
      case _ =>
        throw new IllegalArgumentException(
          s"Unrecognised prefix for primary location: $prefix"
        )
    }

  def createSecondary(prefix: Prefix[_ <: Location]): SecondaryStorageLocation =
    prefix match {
      case s3Prefix: S3ObjectLocationPrefix =>
        SecondaryS3StorageLocation(s3Prefix)
      case azurePrefix: AzureBlobItemLocationPrefix =>
        SecondaryAzureStorageLocation(azurePrefix)
      case _ =>
        throw new IllegalArgumentException(
          s"Unrecognised prefix for primary location: $prefix"
        )
    }
}
