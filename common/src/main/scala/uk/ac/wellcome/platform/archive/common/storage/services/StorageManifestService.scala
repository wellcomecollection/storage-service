package uk.ac.wellcome.platform.archive.common.storage.services

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.common.bagit.services.{BagService, BagUnavailable}
import uk.ac.wellcome.platform.archive.common.ingests.models.{InfrequentAccessStorageProvider, StorageLocation}
import uk.ac.wellcome.platform.archive.common.storage.models.{StorageManifest, StorageSpace}
import uk.ac.wellcome.storage.ObjectLocation


class StorageManifestService(implicit s3Client: AmazonS3) {
  val bagService = new BagService()

  def retrieve(
    bagRootLocation: ObjectLocation,
    storageSpace: StorageSpace
  ): Either[BagUnavailable, StorageManifest] = {
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
