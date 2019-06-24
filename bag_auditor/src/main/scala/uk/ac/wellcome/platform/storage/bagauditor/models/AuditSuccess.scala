package uk.ac.wellcome.platform.storage.bagauditor.models

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier

case class AuditSuccess(
  externalIdentifier: ExternalIdentifier,
  version: Int
)
