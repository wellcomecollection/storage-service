package uk.ac.wellcome.platform.storage.bagauditor.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.operation.models.Summary
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation

case class AuditSummary(
  unpackLocation: ObjectLocation,
  storageSpace: StorageSpace,
  maybeRoot: Option[ObjectLocation] = None,
  startTime: Instant,
  endTime: Option[Instant] = None,
) extends Summary {
  def complete: AuditSummary =
    this.copy(
      endTime = Some(Instant.now())
    )

  def root: ObjectLocation =
    maybeRoot.getOrElse(
      throw new RuntimeException("No root provided by auditor!")
    )
}
