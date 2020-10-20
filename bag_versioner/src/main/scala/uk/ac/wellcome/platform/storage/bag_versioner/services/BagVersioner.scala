package uk.ac.wellcome.platform.storage.bag_versioner.services

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.ingests.models.{IngestID, IngestType}
import uk.ac.wellcome.platform.archive.common.storage.models._
import uk.ac.wellcome.platform.storage.bag_versioner.models._
import uk.ac.wellcome.platform.storage.bag_versioner.versioning._

import scala.util.Try

class BagVersioner(versionPicker: VersionPicker) {
  type IngestStep = Try[IngestStepResult[BagVersionerSummary]]

  def getSummary(
    ingestId: IngestID,
    ingestDate: Instant,
    ingestType: IngestType,
    externalIdentifier: ExternalIdentifier,
    storageSpace: StorageSpace
  ): IngestStep =
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
            BagVersionerSuccessSummary(
              ingestId = ingestId,
              startTime = startTime,
              endTime = Instant.now(),
              version = version
            ),
            maybeUserFacingMessage = Some(s"Assigned bag version $version")
          )

        case Left(error@UnableToAssignVersion(internalError)) =>
          IngestFailed(
            BagVersionerFailureSummary(
              ingestId = ingestId,
              startTime = startTime,
              endTime = Instant.now()
            ),
            e = getUnderlyingThrowable(internalError),
            maybeUserFacingMessage =
              UserFacingMessages.createMessage(ingestId, error)
          )

        case Left(error: FailedToGetLock) =>
          IngestShouldRetry(
            BagVersionerFailureSummary(
              ingestId = ingestId,
              startTime = startTime,
              endTime = Instant.now()
            ),
            e = new Throwable(s"Failed to get lock: ${error.failedLock}")
          )
      }
    }

  private def getUnderlyingThrowable(error: IngestVersionManagerError): Throwable =
    error match {
      case err: IngestVersionManagerDaoError => err.e
      case err => new Throwable(s"Unexpected error in the bag versioner: $err")
    }
}
