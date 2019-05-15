package uk.ac.wellcome.platform.archive.common.storage.services

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.common.bagit.services.BagService
import uk.ac.wellcome.platform.archive.common.ingests.models.{InfrequentAccessStorageProvider, StorageLocation}
import uk.ac.wellcome.platform.archive.common.storage.models.{StorageManifest, StorageSpace}
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.Try


class StorageManifestService(
                              implicit
                              s3Client: AmazonS3
                            ) {

  def retrieve(
                      root: ObjectLocation,
                      space: StorageSpace
  ): Try[StorageManifest] = {

    val bagService = new BagService()

    val locations = List(
      StorageLocation(
        provider = InfrequentAccessStorageProvider,
        location = root
      )
    )

    bagService.retrieve(root).map { bag =>
      StorageManifest.create(root, space, bag, locations)
    }
  }
}
