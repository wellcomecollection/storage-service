package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.models.bagit.BagLocation
import uk.ac.wellcome.platform.archive.common.models.{BagRequest, StorageSpace}

trait BagRequestGenerators extends RandomThings {
  def createBagRequest() = createBagRequestWith()

  def createBagRequestWith(
    bagLocation: BagLocation = createBagLocation): BagRequest =
    BagRequest(
      requestId = randomUUID,
      bagLocation = bagLocation
    )

  def createBagLocation() =
    BagLocation(
      storageNamespace = randomAlphanumeric(),
      storagePrefix = Some(randomAlphanumeric()),
      storageSpace = StorageSpace(randomAlphanumeric()),
      bagPath = randomBagPath)
}
