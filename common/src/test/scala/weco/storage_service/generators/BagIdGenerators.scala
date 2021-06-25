package weco.storage_service.generators

import weco.storage_service.bagit.models.{
  BagId,
  ExternalIdentifier
}
import weco.storage_service.storage.models.StorageSpace

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
