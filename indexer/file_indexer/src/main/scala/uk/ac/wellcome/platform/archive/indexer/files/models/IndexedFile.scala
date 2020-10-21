package uk.ac.wellcome.platform.archive.indexer.files.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.models.FileContext
import uk.ac.wellcome.storage.s3.S3ObjectLocation

case class IndexedFile(
  space: StorageSpace,
  externalIdentifier: ExternalIdentifier,
  location: S3ObjectLocation,
  suffix: Option[String],
  size: Long,
  checksum: IndexedChecksum,
  createdDate: Instant
)

case object IndexedFile {
  def apply(context: FileContext): IndexedFile =
    IndexedFile(
      space = context.space,
      externalIdentifier = context.externalIdentifier,
      location = context.bagLocation.prefix.asLocation(context.file.path),
      suffix = FileSuffix.getSuffix(context.file.name),
      size = context.file.size,
      checksum = IndexedChecksum(
        algorithm = context.hashingAlgorithm.toString,
        value = context.file.checksum.toString
      ),
      createdDate = context.createdDate
    )
}