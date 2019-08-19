package uk.ac.wellcome.platform.archive.common.storage.models

import uk.ac.wellcome.platform.archive.common.ingests.models.StorageProvider
import uk.ac.wellcome.storage.ObjectLocation

sealed trait StorageLocation {
  val provider: StorageProvider
  val location: ObjectLocation
}

case class PrimaryStorageLocation(
  provider: StorageProvider,
  location: ObjectLocation
) extends StorageLocation

case class SecondaryStorageLocation(
  provider: StorageProvider,
  location: ObjectLocation
) extends StorageLocation
