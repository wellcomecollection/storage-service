package uk.ac.wellcome.platform.archive.common.storage.models

import uk.ac.wellcome.storage.{Location, Prefix}

sealed trait StorageLocation {
  val prefix: Prefix[_ <: Location]
}

case class PrimaryStorageLocation(
  prefix: Prefix[_ <: Location]
) extends StorageLocation

case class SecondaryStorageLocation(
  prefix: Prefix[_ <: Location]
) extends StorageLocation
