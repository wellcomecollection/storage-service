package uk.ac.wellcome.platform.archive.common.storage.services

import uk.ac.wellcome.platform.archive.common.bagit.models.Bag
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.{Failure, Try}

class StorageManifestException(message: String) extends RuntimeException(message)

object StorageManifestService {
  def createManifest(
    bag: Bag,
    bagRootLocation: ObjectLocation,
    version: Int
  ): Try[StorageManifest] =
    Failure(
      new Throwable("BOOM!")
    )
}
