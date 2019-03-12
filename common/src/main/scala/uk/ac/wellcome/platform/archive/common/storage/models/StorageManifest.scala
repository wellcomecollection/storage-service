package uk.ac.wellcome.platform.archive.common.storage.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.{BagDigestFile, BagId, BagInfo}
import uk.ac.wellcome.platform.archive.common.ingests.models.StorageLocation

case class ChecksumAlgorithm(value: String) {
  override def toString: String = value
}

case class StorageManifest(
  space: StorageSpace,
  info: BagInfo,
  manifest: FileManifest,
  tagManifest: FileManifest,
  accessLocation: StorageLocation,
  archiveLocations: List[StorageLocation],
  createdDate: Instant
) {
  val id = BagId(space, info.externalIdentifier)
}

case class FileManifest(
  checksumAlgorithm: ChecksumAlgorithm,
  files: List[BagDigestFile]
)
