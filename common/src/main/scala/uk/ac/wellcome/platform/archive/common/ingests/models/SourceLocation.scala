package uk.ac.wellcome.platform.archive.common.ingests.models

import uk.ac.wellcome.storage._

sealed trait NewSourceLocation[SourcePrefix <: Prefix[_]] {
  val provider: StorageProvider
  val prefix: SourcePrefix
}

case class S3SourceLocation(
  prefix: S3ObjectLocationPrefix
) extends NewSourceLocation[S3ObjectLocationPrefix] {
  val provider: StorageProvider = AmazonS3StorageProvider
}

case class AzureBlobSourceLocation(
  prefix: AzureBlobItemLocationPrefix
) extends NewSourceLocation[AzureBlobItemLocationPrefix] {
  val provider: StorageProvider = AzureBlobStorageProvider
}
