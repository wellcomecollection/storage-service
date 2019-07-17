package uk.ac.wellcome.platform.archive.common

import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

case class BagReplicaLocation(
  bagRoot: ObjectLocation,
  versionDirectory: String
) {
  def asLocation: ObjectLocation =
    bagRoot.join(versionDirectory)

  def asPrefix: ObjectLocationPrefix =
    this.asLocation.asPrefix
}

