package weco.storage_service.generators

import weco.storage_service.bagit.models.ExternalIdentifier

trait ExternalIdentifierGenerators extends StorageRandomGenerators {
  // The external identifier can contain slashes, especially in bags whose
  // identifiers come from CALM.
  //
  // Ensure we create identifiers with slashes in tests.
  def createExternalIdentifier: ExternalIdentifier =
    ExternalIdentifier(
      (1 to randomInt(from = 1, to = 5))
        .map { _ =>
          randomAlphanumeric()
        }
        .mkString("/")
    )
}
