package uk.ac.wellcome.platform.storage.bagauditor.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.operation.models.Summary
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation


sealed trait AuditSummary extends Summary {
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
                              ) extends AuditSummary {
}

case class AuditFailureSummary(
                                location: ObjectLocation,
                                space: StorageSpace,
                                startTime: Instant,
                                endTime: Option[Instant]
                              ) extends AuditSummary

case class AuditSuccessSummary(
                                location: ObjectLocation,
                                space: StorageSpace,
                                startTime: Instant,
                                audit: AuditSuccess,
                                endTime: Option[Instant]
                              ) extends AuditSummary {

  val root = audit.root
  val externalIdentifier = audit.externalId

}

object AuditSummary {
  def incomplete(location: ObjectLocation, space: StorageSpace, e: Throwable, t: Instant): AuditIncompleteSummary = {
    AuditIncompleteSummary(location, space, e, t, None)
  }
  def create(
              location: ObjectLocation,
              space: StorageSpace,
              audit: Audit,
              t: Instant
            ): AuditSummary = audit match {
    case f@AuditFailure(e) => AuditFailureSummary(
      location, space, t, Some(Instant.now())
    )
    case s@AuditSuccess(_,_,_) => AuditSuccessSummary(
      location, space, t, s, Some(Instant.now())
    )
  }
}

sealed trait Audit
case class AuditSuccess(root: ObjectLocation, externalId: ExternalIdentifier, version: Int) extends Audit
case class AuditFailure(e: Throwable) extends Audit