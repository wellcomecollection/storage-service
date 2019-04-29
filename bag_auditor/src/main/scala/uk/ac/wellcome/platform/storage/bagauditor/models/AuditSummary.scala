package uk.ac.wellcome.platform.storage.bagauditor.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.operation.models.Summary
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation

case class AuditInformation(
  bagRootLocation: ObjectLocation,
  externalIdentifier: ExternalIdentifier
)

case class AuditSummary(
  unpackLocation: ObjectLocation,
  storageSpace: StorageSpace,
  maybeAuditInformation: Option[AuditInformation] = None,
  startTime: Instant,
  endTime: Option[Instant] = None,
) extends Summary {
  def complete: AuditSummary =
    this.copy(
      endTime = Some(Instant.now())
    )

  def auditInformation: AuditInformation =
    maybeAuditInformation.getOrElse(
      throw new RuntimeException("No info provided by auditor!")
    )

  def root: ObjectLocation = auditInformation.bagRootLocation

  def externalIdentifier: ExternalIdentifier =
    auditInformation.externalIdentifier
}
