package uk.ac.wellcome.platform.archive.common

import uk.ac.wellcome.storage.ObjectLocation

case class BagReplicaLocation(
  bagRoot: ObjectLocation,
  versionDirectory: String
) {
  def asLocation: ObjectLocation =
    bagRoot.join(versionDirectory)
}

