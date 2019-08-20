package uk.ac.wellcome.platform.archive.common.storage.models

import uk.ac.wellcome.platform.archive.common.ingests.models.StorageProvider
import uk.ac.wellcome.storage.ObjectLocationPrefix

sealed trait StorageLocation {
  val provider: StorageProvider
  val prefix: ObjectLocationPrefix
}

case class PrimaryStorageLocation(
  provider: StorageProvider,
  prefix: ObjectLocationPrefix
) extends StorageLocation

case class SecondaryStorageLocation(
  provider: StorageProvider,
  prefix: ObjectLocationPrefix
) extends StorageLocation
