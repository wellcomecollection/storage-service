package uk.ac.wellcome.platform.storage.bagauditor.services

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  IngestID,
  IngestType
}
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestFailed,
  IngestStepResult,
  IngestStepSucceeded,
  StorageSpace
}
import uk.ac.wellcome.platform.archive.common.versioning.IngestVersionManagerDaoError
import uk.ac.wellcome.platform.storage.bagauditor.models._
import uk.ac.wellcome.platform.storage.bagauditor.versioning.{
  UnableToAssignVersion,
  VersionPicker,
  VersionPickerError
}

import scala.util.Try

class BagAuditor(versionPicker: VersionPicker) {
  type IngestStep = Try[IngestStepResult[AuditSummary]]

  def getAuditSummary(ingestId: IngestID,
                      ingestDate: Instant,
                      ingestType: IngestType,
                      externalIdentifier: ExternalIdentifier,
                      storageSpace: StorageSpace): IngestStep =
    Try {
      val startTime = Instant.now()

      val maybeVersion = versionPicker.chooseVersion(
        externalIdentifier = externalIdentifier,
        ingestId = ingestId,
        ingestType = ingestType,
        ingestDate = ingestDate,
        storageSpace = storageSpace
      )

      maybeVersion match {
        case Right(version) =>
          IngestStepSucceeded(
            AuditSuccessSummary(
              startTime = startTime,
              endTime = Instant.now(),
              version = version
            ),
            maybeUserFacingMessage = Some(s"Assigned bag version $version")
          )

        case Left(auditError) =>
          IngestFailed(
            AuditFailureSummary(
              startTime = startTime,
              endTime = Instant.now()
            ),
            e = getUnderlyingThrowable(auditError),
            maybeUserFacingMessage =
              UserFacingMessages.createMessage(ingestId, auditError)
          )
      }
    }

  private def getUnderlyingThrowable(error: VersionPickerError): Throwable =
    error match {
      case UnableToAssignVersion(internalError: IngestVersionManagerDaoError) =>
        internalError.e
      case err => new Throwable(s"Unexpected error in the auditor: $err")
    }
}
