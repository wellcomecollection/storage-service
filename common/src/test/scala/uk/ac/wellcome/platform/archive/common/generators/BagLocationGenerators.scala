package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.bagit.models.BagLocation
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings

trait BagLocationGenerators extends RandomThings with StorageSpaceGenerators {
  def createBagLocation() =
    BagLocation(
      storageNamespace = randomAlphanumeric(),
      storagePrefix = Some(randomAlphanumeric()),
      storageSpace = createStorageSpace,
      bagPath = randomBagPath)
}
