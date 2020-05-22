package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings

trait ExternalIdentifierGenerators extends StorageRandomThings {
  // The external identifier can contain slashes, especially in bags whose
  // identifiers come from CALM.
  //
  // Ensure we create identifiers with slashes in tests.
  def createExternalIdentifier: ExternalIdentifier =
    ExternalIdentifier(
      (1 to randomInt(from = 1, to = 5))
        .map { _ => randomAlphanumericWithLength() }
        .mkString("/")
    )
}
