package uk.ac.wellcome.platform.archive.common.ingests.models

import uk.ac.wellcome.storage._

sealed trait SourceLocation {
  val provider: StorageProvider
  val location: Location
}

case class S3SourceLocation(
  location: S3ObjectLocation
) extends SourceLocation {
  val provider: StorageProvider = AmazonS3StorageProvider
}
