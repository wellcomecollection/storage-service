package uk.ac.wellcome.platform.storage.bagauditor.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.operation.models.Summary

sealed trait AuditSummary extends Summary

case class AuditFailureSummary(
  startTime: Instant,
  endTime: Option[Instant]
) extends AuditSummary

case class AuditSuccessSummary(
  startTime: Instant,
  endTime: Option[Instant],
  version: Int
) extends AuditSummary
