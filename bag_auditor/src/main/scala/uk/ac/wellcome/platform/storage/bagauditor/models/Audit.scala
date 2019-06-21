package uk.ac.wellcome.platform.storage.bagauditor.models

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier

sealed trait Audit

case class AuditSuccess(
  externalIdentifier: ExternalIdentifier,
  version: Int
) extends Audit

case class AuditFailure(e: Throwable) extends Audit
