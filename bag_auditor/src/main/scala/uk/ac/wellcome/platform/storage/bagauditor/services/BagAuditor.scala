package uk.ac.wellcome.platform.storage.bagauditor.services

import java.time.Instant

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagInfo, ExternalIdentifier}
import uk.ac.wellcome.platform.archive.common.ingests.models.{CreateIngestType, IngestID, IngestType}
import uk.ac.wellcome.platform.archive.common.storage.StreamUnavailable
import uk.ac.wellcome.platform.archive.common.storage.models.{IngestFailed, IngestStepResult, IngestStepSucceeded, StorageSpace}
import uk.ac.wellcome.platform.storage.bagauditor.models._
import uk.ac.wellcome.platform.archive.common.storage.services.S3BagLocator
import uk.ac.wellcome.platform.archive.common.storage.services.S3StreamableInstances._
import uk.ac.wellcome.platform.storage.bagauditor.versioning.VersionPicker
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.{Failure, Success, Try}

class BagAuditor(versionPicker: VersionPicker)(implicit s3Client: AmazonS3) extends Logging {
  val s3BagLocator = new S3BagLocator(s3Client)

  type IngestStep = Try[IngestStepResult[AuditSummary]]

  def getAuditSummary(ingestId: IngestID,
                      ingestDate: Instant,
                      ingestType: IngestType = CreateIngestType,
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
      case _ => new Throwable()
    }

  private def createUserFacingMessage(ingestId: IngestID, auditError: AuditError): Option[String] =
    auditError match {
      case CannotFindExternalIdentifier(err) =>
        info(s"$ingestId: unable to find an external identifier. Error: $err")
        Some("Unable to find an external identifier")

      case IngestTypeUpdateForNewBag() =>
        info(s"$ingestId: ingestType = 'update' but no existing version")
        Some("This bag has never been ingested before, but was sent with ingestType update")

      case IngestTypeCreateForExistingBag() =>
        Some("This bag has already been ingested, but was sent with ingestType create")

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
