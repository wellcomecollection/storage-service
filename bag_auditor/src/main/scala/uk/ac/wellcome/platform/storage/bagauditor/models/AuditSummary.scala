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

case class AuditIncompleteSummary(
  root: ObjectLocation,
  space: StorageSpace,
  e: Throwable,
  startTime: Instant,
  endTime: Option[Instant] = None
) extends AuditSummary

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

case object AuditSummary {
  def incomplete(root: ObjectLocation,
                 space: StorageSpace,
                 e: Throwable,
                 t: Instant): AuditIncompleteSummary =
    AuditIncompleteSummary(
      root = root,
      space = space,
      e = e,
      startTime = t,
      endTime = None
    )

  def create(
    root: ObjectLocation,
    space: StorageSpace,
    audit: Audit,
    t: Instant
  ): AuditSummary = audit match {
    case f @ AuditFailure(e) =>
      AuditFailureSummary(
        root = root,
        space = space,
        startTime = t,
        endTime = Some(Instant.now())
      )
    case s @ AuditSuccess(_, _) =>
      AuditSuccessSummary(
        root = root,
        space = space,
        startTime = t,
        audit = s,
        endTime = Some(Instant.now())
      )
  }
}
