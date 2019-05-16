package uk.ac.wellcome.platform.storage.bagauditor.models

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.storage.ObjectLocation

sealed trait BetterAudit

case class AuditSuccess(
  root: ObjectLocation,
  externalIdentifier: ExternalIdentifier,
  version: Int
) extends BetterAudit

case class AuditFailure(e: Throwable) extends BetterAudit
