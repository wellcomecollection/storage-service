package uk.ac.wellcome.platform.archive.common.storage.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagId,
  BagInfo,
  BagVersion
}
import uk.ac.wellcome.platform.archive.common.ingests.models.StorageLocation
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
  location: BetterStorageLocation,
  replicaLocations: Seq[BetterStorageLocation],
  createdDate: Instant
) {
  val id = BagId(space, info.externalIdentifier)

  // TODO: Remove this converter when we modify the bags API and notifier
  // to send the correct format.
  private def toOldLocation(location: BetterStorageLocation): StorageLocation =
    StorageLocation(
      provider = location.provider,
      location = location.location
    )

  def locations: Seq[StorageLocation] =
    (Seq(location) ++ replicaLocations).map { toOldLocation }
}
