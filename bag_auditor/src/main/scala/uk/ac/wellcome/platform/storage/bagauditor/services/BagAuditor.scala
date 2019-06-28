package uk.ac.wellcome.platform.storage.bagauditor.services

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.ingests.models.{IngestID, IngestType}
import uk.ac.wellcome.platform.archive.common.storage.models.{IngestFailed, IngestStepResult, IngestStepSucceeded, StorageSpace}
import uk.ac.wellcome.platform.archive.common.versioning.IngestVersionManagerDaoError
import uk.ac.wellcome.platform.storage.bagauditor.models._
import uk.ac.wellcome.platform.storage.bagauditor.versioning.VersionPicker
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.Try

class BagAuditor(versionPicker: VersionPicker) {
  type IngestStep = Try[IngestStepResult[AuditSummary]]

  def getAuditSummary(ingestId: IngestID,
                      ingestDate: Instant,
                      ingestType: IngestType,
                      externalIdentifier: ExternalIdentifier,
                      root: ObjectLocation,
                      storageSpace: StorageSpace): IngestStep =
    Try {
      val startTime = Instant.now()

      val audit: Either[AuditError, AuditSuccess] = for {
        version <- versionPicker.chooseVersion(
          externalIdentifier = externalIdentifier,
          ingestId = ingestId,
          ingestType = ingestType,
          ingestDate = ingestDate,
          storageSpace = storageSpace
        )
        auditSuccess = AuditSuccess(
          version = version
        )
      } yield auditSuccess

      audit match {
        case Right(auditSuccess) =>
          IngestStepSucceeded(
            AuditSuccessSummary(
              root = root,
              space = storageSpace,
              startTime = startTime,
              audit = auditSuccess,
              endTime = Some(Instant.now())
            )
          )

        case Left(auditError) =>
          IngestFailed(
            AuditFailureSummary(
              root = root,
              space = storageSpace,
              startTime = startTime,
              endTime = Some(Instant.now())
            ),
            e = getUnderlyingThrowable(auditError),
            maybeUserFacingMessage =
              UserFacingMessages.createMessage(ingestId, auditError)
          )
      }
    }

  private def getUnderlyingThrowable(auditError: AuditError): Throwable =
    auditError match {
      case CannotFindExternalIdentifier(e) => e
      case UnableToAssignVersion(internalError: IngestVersionManagerDaoError) =>
        internalError.e
      case err => new Throwable(s"Unexpected error in the auditor: $err")
    }
}
