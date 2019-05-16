package uk.ac.wellcome.platform.storage.bagauditor.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.operation.models.Summary
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation

sealed trait BetterAuditSummary extends Summary {
  val location: ObjectLocation
  val space: StorageSpace
  val startTime: Instant
}

case class AuditIncompleteSummary(
  location: ObjectLocation,
  space: StorageSpace,
  e: Throwable,
  startTime: Instant,
  endTime: Option[Instant] = None
) extends BetterAuditSummary

case class AuditFailureSummary(
  location: ObjectLocation,
  space: StorageSpace,
  startTime: Instant,
  endTime: Option[Instant]
) extends BetterAuditSummary

case class AuditSuccessSummary(
  location: ObjectLocation,
  space: StorageSpace,
  startTime: Instant,
  audit: AuditSuccess,
  endTime: Option[Instant]
) extends BetterAuditSummary

case object BetterAuditSummary {
  def incomplete(
    location: ObjectLocation,
    space: StorageSpace,
    e: Throwable,
    t: Instant): AuditIncompleteSummary =
    AuditIncompleteSummary(
      location = location,
      space = space,
      e = e,
      startTime = t,
      endTime = None
    )

  def create(
    location: ObjectLocation,
    space: StorageSpace,
    audit: BetterAudit,
    t: Instant
  ): BetterAuditSummary = audit match {
    case f @ AuditFailure(e) =>
      AuditFailureSummary(
        location = location,
        space = space,
        startTime = t,
        endTime = Some(Instant.now())
      )
    case s @ AuditSuccess(_, _, _) =>
      AuditSuccessSummary(
        location = location,
        space = space,
        startTime = t,
        audit = s,
        endTime = Some(Instant.now())
      )
  }
}