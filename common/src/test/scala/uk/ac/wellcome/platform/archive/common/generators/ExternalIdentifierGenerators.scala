package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings

trait ExternalIdentifierGenerators extends RandomThings {
  def createExternalIdentifier: ExternalIdentifier =
    ExternalIdentifier(randomAlphanumeric())
}
