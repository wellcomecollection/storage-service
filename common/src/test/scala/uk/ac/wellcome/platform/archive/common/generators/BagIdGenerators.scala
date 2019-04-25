package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.bagit.models.BagId

trait BagIdGenerators
    extends ExternalIdentifierGenerators
    with StorageSpaceGenerators {
  def createBagId = BagId(
    space = createStorageSpace,
    externalIdentifier = createExternalIdentifier
  )
}
