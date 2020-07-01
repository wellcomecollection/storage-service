package uk.ac.wellcome.platform.archive.display

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.platform.archive.common.storage.models.StorageLocation
import uk.ac.wellcome.storage.{AzureBlobItemLocationPrefix, Prefix, S3ObjectLocationPrefix}

case class DisplayLocation(
  provider: DisplayProvider,
  bucket: String,
  path: String,
  @JsonKey("type") ontologyType: String = "Location"
) {
  def toSourceLocation: SourceLocation[_ <: Prefix[_]] =
    provider.toStorageProvider match {
      case AmazonS3StorageProvider =>
        S3SourceLocation(
          prefix = S3ObjectLocationPrefix(bucket = bucket, keyPrefix = path)
        )

      case AzureBlobStorageProvider =>
        AzureBlobSourceLocation(
          prefix = AzureBlobItemLocationPrefix(container = bucket, namePrefix = path)
        )
    }
}

object DisplayLocation {
  def apply(location: SourceLocation[_]): DisplayLocation =
    location match {
      case S3SourceLocation(prefix) =>
        DisplayLocation(
          provider = DisplayProvider(location.provider),
          bucket = prefix.bucket,
          path = prefix.keyPrefix
        )

      case AzureBlobSourceLocation(prefix) =>
        DisplayLocation(
          provider = DisplayProvider(location.provider),
          bucket = prefix.container,
          path = prefix.namePrefix
        )
    }

  def apply(location: StorageLocation): DisplayLocation =
    DisplayLocation(
      provider = DisplayProvider(location.provider),
      bucket = location.prefix.namespace,
      path = location.prefix.path
    )
}
