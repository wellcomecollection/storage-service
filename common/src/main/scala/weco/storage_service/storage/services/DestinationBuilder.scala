package weco.storage_service.storage.services

import java.nio.file.Paths

import weco.storage_service.bagit.models.{
  BagVersion,
  ExternalIdentifier
}
import weco.storage_service.storage.models.StorageSpace

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
