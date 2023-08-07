package weco.storage_service.ingests.models

import weco.storage.Location
import weco.storage.providers.s3.S3ObjectLocation

sealed trait SourceLocation {
  val provider: StorageProvider
  val location: Location
}

case class S3SourceLocation(
  location: S3ObjectLocation
) extends SourceLocation {
  val provider: StorageProvider = AmazonS3StorageProvider
}
