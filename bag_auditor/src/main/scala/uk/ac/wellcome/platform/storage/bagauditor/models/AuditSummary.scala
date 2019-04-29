package uk.ac.wellcome.platform.storage.bagauditor.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.operation.models.Summary
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation

case class AuditInformation(
  bagRoot: ObjectLocation
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

  def root: ObjectLocation =
    auditInformation.bagRoot
}
