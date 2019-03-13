package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.bagit.models

trait BagIdGenerators
    extends ExternalIdentifierGenerators
    with StorageSpaceGenerators {
  def createBagId = models.BagId(
    space = createStorageSpace,
    externalIdentifier = createExternalIdentifier
  )
}
