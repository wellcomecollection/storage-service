package uk.ac.wellcome.platform.archive.indexer.elasticsearch.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.storage.models.{PrimaryS3StorageLocation, StorageManifestFile, StorageSpace}
import uk.ac.wellcome.platform.archive.common.verify.HashingAlgorithm
import uk.ac.wellcome.storage.s3.S3ObjectLocation

case class FileContext(
  space: StorageSpace,
  externalIdentifier: ExternalIdentifier,
  hashingAlgorithm: HashingAlgorithm,
  bagLocation: PrimaryS3StorageLocation,
  file: StorageManifestFile,
  createdDate: Instant
) {
  def location: S3ObjectLocation =
    bagLocation.prefix.asLocation(file.path)
}
