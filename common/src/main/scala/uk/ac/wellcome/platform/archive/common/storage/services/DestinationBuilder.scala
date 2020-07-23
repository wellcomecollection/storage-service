package uk.ac.wellcome.platform.archive.common.storage.services

import java.nio.file.Paths

import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagVersion,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace

object DestinationBuilder {
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
