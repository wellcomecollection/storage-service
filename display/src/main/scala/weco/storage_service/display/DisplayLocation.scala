package weco.storage_service.display

import io.circe.generic.extras.JsonKey
import weco.storage_service.ingests.models._
import weco.storage_service.storage.models.StorageLocation
import weco.storage.providers.s3.S3ObjectLocation

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
