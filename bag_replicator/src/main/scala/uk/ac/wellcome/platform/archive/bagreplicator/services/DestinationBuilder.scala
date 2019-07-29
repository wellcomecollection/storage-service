package uk.ac.wellcome.platform.archive.bagreplicator.services

import java.nio.file.Paths

import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagVersion,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocationPrefix

class DestinationBuilder(namespace: String, rootPath: Option[String]) {
  def buildDestination(
    storageSpace: StorageSpace,
    externalIdentifier: ExternalIdentifier,
    version: BagVersion
  ): ObjectLocationPrefix = ObjectLocationPrefix(
    namespace = namespace,
    path = Paths
      .get(
        rootPath.getOrElse(""),
        storageSpace.toString,
        externalIdentifier.toString,
        version.toString
      )
      .toString
  )
}
