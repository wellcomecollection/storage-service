package uk.ac.wellcome.platform.archive.common.ingests.models

import uk.ac.wellcome.storage._

sealed trait SourceLocation[SourcePrefix <: Prefix[_]] {
  val provider: StorageProvider
  val prefix: SourcePrefix
}

case class S3SourceLocation(
  prefix: S3ObjectLocationPrefix
) extends SourceLocation[S3ObjectLocationPrefix] {
  val provider: StorageProvider = AmazonS3StorageProvider
}

case class AzureBlobSourceLocation(
  prefix: AzureBlobItemLocationPrefix
) extends SourceLocation[AzureBlobItemLocationPrefix] {
  val provider: StorageProvider = AzureBlobStorageProvider
}
