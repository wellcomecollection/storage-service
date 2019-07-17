package uk.ac.wellcome.platform.storage.bagauditor.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.operation.models.Summary

sealed trait AuditSummary extends Summary {
  val endTime: Instant
  override val maybeEndTime: Option[Instant] = Some(endTime)
}

case class AuditFailureSummary(
  startTime: Instant,
  endTime: Instant
) extends AuditSummary

case class AuditSuccessSummary(
  startTime: Instant,
  endTime: Instant,
  version: Int
) extends AuditSummary
