package uk.ac.wellcome.platform.archive.common.storage.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagId,
  BagInfo,
  BagVersion
}
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.verify.{
  ChecksumValue,
  HashingAlgorithm
}

case class StorageManifestFile(
  checksum: ChecksumValue,
  name: String,
  path: String,
  size: Long
)

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
  location: PrimaryStorageLocation,
  replicaLocations: Seq[SecondaryStorageLocation],
  createdDate: Instant,
  ingestId: IngestID
) {
  val id = BagId(space, info.externalIdentifier)
  val idWithVersion = s"${id}/${version}"
}
