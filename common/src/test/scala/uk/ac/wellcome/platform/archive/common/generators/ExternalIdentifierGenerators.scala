package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings

trait ExternalIdentifierGenerators extends StorageRandomThings {
  def createExternalIdentifier: ExternalIdentifier =
    ExternalIdentifier(randomAlphanumericWithLength())
}
