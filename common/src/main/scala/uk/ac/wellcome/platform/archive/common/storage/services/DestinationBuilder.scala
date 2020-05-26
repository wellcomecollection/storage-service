package uk.ac.wellcome.platform.archive.common.storage.services

import java.nio.file.Paths

import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagVersion,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocationPrefix

object DestinationBuilder {
  def buildDestination(
    namespace: String,
    storageSpace: StorageSpace,
    externalIdentifier: ExternalIdentifier,
    version: BagVersion
  ): ObjectLocationPrefix = ObjectLocationPrefix(
    namespace = namespace,
    path = buildPath(storageSpace, externalIdentifier, version)
  )

  def buildPath(
    storageSpace: StorageSpace,
    externalIdentifier: ExternalIdentifier,
    version: BagVersion
  ): String =
    Paths
      .get(
        storageSpace.toString,
        externalIdentifier.toString,
        version.toString
      )
      .toString
}
