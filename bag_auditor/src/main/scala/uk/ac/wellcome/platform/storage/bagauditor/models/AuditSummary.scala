package uk.ac.wellcome.platform.storage.bagauditor.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.operation.models.Summary
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation

sealed trait AuditSummary extends Summary {
  val root: ObjectLocation
  val space: StorageSpace
  val startTime: Instant
}

case class AuditFailureSummary(
  root: ObjectLocation,
  space: StorageSpace,
  startTime: Instant,
  endTime: Option[Instant]
) extends AuditSummary

case class AuditSuccessSummary(
  root: ObjectLocation,
  space: StorageSpace,
  startTime: Instant,
  audit: AuditSuccess,
  endTime: Option[Instant]
) extends AuditSummary
