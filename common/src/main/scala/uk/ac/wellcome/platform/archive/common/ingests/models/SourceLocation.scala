package uk.ac.wellcome.platform.archive.common.ingests.models

import uk.ac.wellcome.storage._

sealed trait SourceLocation {
  val provider: StorageProvider
  val prefix: Prefix[_]
}

case class S3SourceLocation(
  prefix: S3ObjectLocationPrefix
) extends SourceLocation {
  val provider: StorageProvider = AmazonS3StorageProvider
}

case class AzureBlobSourceLocation(
  prefix: AzureBlobItemLocationPrefix
) extends SourceLocation {
  val provider: StorageProvider = AzureBlobStorageProvider
}
