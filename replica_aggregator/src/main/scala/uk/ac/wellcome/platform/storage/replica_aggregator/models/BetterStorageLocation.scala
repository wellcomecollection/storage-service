package uk.ac.wellcome.platform.storage.replica_aggregator.models

import uk.ac.wellcome.platform.archive.common.ingests.models.StorageProvider
import uk.ac.wellcome.storage.ObjectLocation

// replaces StorageLocation
sealed trait BetterStorageLocation {
  val provider: StorageProvider
  val location: ObjectLocation
}

case class PrimaryStorageLocation(provider: StorageProvider, location: ObjectLocation) extends BetterStorageLocation
case class SecondaryStorageLocation(provider: StorageProvider, location: ObjectLocation) extends BetterStorageLocation
