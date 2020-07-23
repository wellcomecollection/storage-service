package uk.ac.wellcome.platform.archive.display

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.platform.archive.common.storage.models.{
  AzureStorageLocation,
  S3StorageLocation,
  StorageLocation
}
import uk.ac.wellcome.storage.S3ObjectLocation

case class DisplayLocation(
  provider: DisplayProvider,
  bucket: String,
  path: String,
  @JsonKey("type") ontologyType: String = "Location"
) {
  def toSourceLocation: SourceLocation =
    provider.toStorageProvider match {
      case AmazonS3StorageProvider =>
        S3SourceLocation(
          location = S3ObjectLocation(bucket = bucket, key = path)
        )

      case AzureBlobStorageProvider =>
        throw new IllegalArgumentException(
          "Unpacking is not supported from an Azure location!"
        )
    }
}

object DisplayLocation {
  def apply(location: SourceLocation): DisplayLocation =
    location match {
      case S3SourceLocation(s3Location) =>
        DisplayLocation(
          provider = DisplayProvider(location.provider),
          bucket = s3Location.bucket,
          path = s3Location.key
        )
    }

  def apply(location: StorageLocation): DisplayLocation =
    DisplayLocation(
      provider = DisplayProvider(location.provider),
      bucket = location.prefix.namespace,
      path = location.prefix.pathPrefix
    )
}
