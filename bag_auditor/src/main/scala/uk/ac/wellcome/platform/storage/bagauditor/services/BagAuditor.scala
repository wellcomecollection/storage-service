package uk.ac.wellcome.platform.storage.bagauditor.services

import java.time.Instant

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagInfo, ExternalIdentifier}
import uk.ac.wellcome.platform.archive.common.ingests.models.{IngestID, IngestType}
import uk.ac.wellcome.platform.archive.common.storage.StreamUnavailable
import uk.ac.wellcome.platform.archive.common.storage.models.{IngestFailed, IngestStepResult, IngestStepSucceeded, StorageSpace}
import uk.ac.wellcome.platform.archive.common.storage.services.S3BagLocator
import uk.ac.wellcome.platform.archive.common.storage.services.S3StreamableInstances._
import uk.ac.wellcome.platform.archive.common.versioning.{ExternalIdentifiersMismatch, InternalVersionManagerError, NewerIngestAlreadyExists}
import uk.ac.wellcome.platform.storage.bagauditor.models._
import uk.ac.wellcome.platform.storage.bagauditor.versioning.VersionPicker
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.{Failure, Success, Try}

class BagAuditor(versionPicker: VersionPicker)(implicit s3Client: AmazonS3) extends Logging {
  val s3BagLocator = new S3BagLocator(s3Client)

  type IngestStep = Try[IngestStepResult[AuditSummary]]

  def getAuditSummary(ingestId: IngestID,
                      ingestDate: Instant,
                      ingestType: IngestType,
                      root: ObjectLocation,
                      storageSpace: StorageSpace): IngestStep =
    Try {
      val startTime = Instant.now()

      val audit: Either[AuditError, AuditSuccess] = for {
        externalIdentifier <- getBagIdentifier(root)
        version <- versionPicker.chooseVersion(
          externalIdentifier = externalIdentifier,
          ingestId = ingestId,
          ingestType = ingestType,
          ingestDate = ingestDate
        )
        auditSuccess = AuditSuccess(
          externalIdentifier = externalIdentifier,
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
            maybeUserFacingMessage = createUserFacingMessage(ingestId, auditError)
          )
      }
    }

  private def getUnderlyingThrowable(auditError: AuditError): Throwable =
    auditError match {
      case CannotFindExternalIdentifier(e) => e
      case UnableToAssignVersion(internalError: InternalVersionManagerError)
                                           => internalError.e
      case err                             => new Throwable(s"Unexpected error in the auditor: $err")
    }

  private def createUserFacingMessage(ingestId: IngestID, auditError: AuditError): Option[String] =
    auditError match {
      case CannotFindExternalIdentifier(err) =>
        info(s"Unable to find an external identifier for $ingestId. Error: $err")
        Some("Unable to find an external identifier")

      case IngestTypeUpdateForNewBag() =>
        Some("This bag has never been ingested before, but was sent with ingestType update")

      case IngestTypeCreateForExistingBag() =>
        Some("This bag has already been ingested, but was sent with ingestType create")

      case UnableToAssignVersion(e: NewerIngestAlreadyExists) =>
        Some(s"Another version of this bag was ingested at ${e.stored}, which is newer than the current ingest ${e.request}")

      // This should be impossible, and it strongly points to an error somewhere in
      // the pipeline -- an ingest ID should be used once, and the underlying bag
      // shouldn't change!  We don't bubble up an error because it's an internal failure,
      // and there's nothing the user can do about it.
      case UnableToAssignVersion(e: ExternalIdentifiersMismatch) =>
        warn(s"External identifiers mismatch for $ingestId: $e")
        None

      case _ => None
    }

  private def getBagIdentifier(
    bagRootLocation: ObjectLocation): Either[CannotFindExternalIdentifier, ExternalIdentifier] = {

    val tryExternalIdentifier =
      for {
        bagInfoLocation <- s3BagLocator.locateBagInfo(bagRootLocation)
        inputStream <- bagInfoLocation.toInputStream match {
          case Left(e)                  => Failure(e)
          case Right(None)              => Failure(StreamUnavailable("No stream available!"))
          case Right(Some(inputStream)) => Success(inputStream)
        }
        bagInfo <- BagInfo.create(inputStream)
      } yield bagInfo.externalIdentifier

    tryExternalIdentifier match {
      case Success(externalIdentifier) => Right(externalIdentifier)
      case Failure(err)                => Left(CannotFindExternalIdentifier(err))
    }
  }
}
