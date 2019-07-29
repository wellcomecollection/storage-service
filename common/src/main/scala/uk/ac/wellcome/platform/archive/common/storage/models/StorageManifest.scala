package uk.ac.wellcome.platform.archive.common.storage.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagFile,
  BagId,
  BagInfo,
  BagVersion
}
import uk.ac.wellcome.platform.archive.common.ingests.models.StorageLocation
import uk.ac.wellcome.platform.archive.common.verify.{
  ChecksumValue,
  HashingAlgorithm
}

// TODO: These need better names!
case class StorageManifestFile(
  checksum: ChecksumValue,
  name: String,
  path: String
)

case object StorageManifestFile {
  // TODO: Ditch this, temporary method
  def apply(bagFile: BagFile): StorageManifestFile =
    StorageManifestFile(
      checksum = bagFile.checksum.value,
      name = bagFile.path.value,
      path = bagFile.path.value
    )
}

case class FileManifest(
  checksumAlgorithm: HashingAlgorithm,
  files: Seq[StorageManifestFile]
)

case class StorageManifest(
  space: StorageSpace,
  info: BagInfo,
  version: BagVersion,
  manifest: FileManifest,
  tagManifest: FileManifest,
  locations: Seq[StorageLocation],
  createdDate: Instant
) {
  val id = BagId(space, info.externalIdentifier)
}
