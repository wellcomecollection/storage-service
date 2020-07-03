package uk.ac.wellcome.platform.archive.common.storage.models

import uk.ac.wellcome.storage.{Location, Prefix}

sealed trait ReplicaLocation {
  val prefix: Prefix[_ <: Location]
}

case class PrimaryReplicaLocation(
  prefix: Prefix[_ <: Location]
) extends ReplicaLocation

case class SecondaryReplicaLocation(
  prefix: Prefix[_ <: Location]
) extends ReplicaLocation
