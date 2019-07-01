package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, ExternalIdentifier}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace

trait BagIdGenerators
    extends ExternalIdentifierGenerators
    with StorageSpaceGenerators {
  def createBagIdWith(
    space: StorageSpace = createStorageSpace,
    externalIdentifier: ExternalIdentifier = createExternalIdentifier
  ): BagId =
    BagId(space, externalIdentifier)

  def createBagId: BagId = createBagIdWith()
}
