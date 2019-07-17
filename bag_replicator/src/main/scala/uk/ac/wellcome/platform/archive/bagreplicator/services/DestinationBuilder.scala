package uk.ac.wellcome.platform.archive.bagreplicator.services

import java.nio.file.Paths

import uk.ac.wellcome.platform.archive.common.BagReplicaLocation
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation

class DestinationBuilder(namespace: String, rootPath: Option[String]) {
  def buildDestination(
    storageSpace: StorageSpace,
    externalIdentifier: ExternalIdentifier,
    version: Int
  ): BagReplicaLocation = BagReplicaLocation(
    bagRoot = ObjectLocation(
      namespace = namespace,
      path = Paths
        .get(
          rootPath.getOrElse(""),
          storageSpace.toString,
          externalIdentifier.toString,
        )
        .toString
    ),
    versionDirectory = s"v$version"
  )
}
