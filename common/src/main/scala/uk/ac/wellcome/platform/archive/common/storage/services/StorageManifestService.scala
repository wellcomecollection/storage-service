package uk.ac.wellcome.platform.archive.common.storage.services

import uk.ac.wellcome.platform.archive.common.bagit.services.BagService
import uk.ac.wellcome.platform.archive.common.ingests.models.{InfrequentAccessStorageProvider, StorageLocation}
import uk.ac.wellcome.platform.archive.common.storage.models.{StorageManifest, StorageSpace}
import uk.ac.wellcome.storage.{ObjectLocation, StorageBackend}

import scala.util.Try

class StorageManifestService(implicit storageBackend: StorageBackend) {
  val bagService = new BagService()

  def retrieve(
    bagRootLocation: ObjectLocation,
    storageSpace: StorageSpace
  ): Try[StorageManifest] = {
    val locations = List(
      StorageLocation(
        provider = InfrequentAccessStorageProvider,
        location = bagRootLocation
      )
    )

    bagService.retrieve(bagRootLocation).map { bag =>
      StorageManifest.create(bagRootLocation, storageSpace, bag, locations)
    }
  }
}
