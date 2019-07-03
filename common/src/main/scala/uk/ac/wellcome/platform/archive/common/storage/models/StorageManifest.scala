package uk.ac.wellcome.platform.archive.common.storage.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagId,
  BagInfo,
  BagManifest
}
import uk.ac.wellcome.platform.archive.common.ingests.models.StorageLocation

case class StorageManifest(
  space: StorageSpace,
  info: BagInfo,
  version: Int,
  manifest: BagManifest,
  tagManifest: BagManifest,
  locations: List[StorageLocation],
  createdDate: Instant
) {
  val id = BagId(space, info.externalIdentifier)
}
