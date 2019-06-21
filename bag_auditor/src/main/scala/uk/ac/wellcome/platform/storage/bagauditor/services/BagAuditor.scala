package uk.ac.wellcome.platform.storage.bagauditor.services

import java.time.Instant

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagInfo,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.StreamUnavailable
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestFailed,
  IngestStepResult,
  IngestStepSucceeded,
  StorageSpace
}
import uk.ac.wellcome.platform.storage.bagauditor.models._
import uk.ac.wellcome.platform.archive.common.storage.services.S3BagLocator
import uk.ac.wellcome.platform.archive.common.storage.services.S3StreamableInstances._
import uk.ac.wellcome.platform.storage.bagauditor.versioning.VersionPicker
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.{Failure, Success, Try}

class BagAuditor(versionPicker: VersionPicker)(implicit s3Client: AmazonS3) {
  val s3BagLocator = new S3BagLocator(s3Client)

  type IngestStep = Try[IngestStepResult[AuditSummary]]

  def getAuditSummary(ingestId: IngestID,
                      ingestDate: Instant,
                      root: ObjectLocation,
                      storageSpace: StorageSpace): IngestStep =
    Try {
      val startTime = Instant.now()

      val auditTry: Try[AuditSuccess] = for {
        externalIdentifier <- getBagIdentifier(root)
        version <- versionPicker.chooseVersion(
          externalIdentifier = externalIdentifier,
          ingestId = ingestId,
          ingestDate = ingestDate
        )
        auditSuccess = AuditSuccess(
          externalIdentifier = externalIdentifier,
          version = version
        )
      } yield auditSuccess

      auditTry match {
        case Success(auditSuccess) =>
          IngestStepSucceeded(
            AuditSummary.create(
              root = root,
              space = storageSpace,
              audit = auditSuccess,
              t = startTime
            )
          )
        case Failure(err) =>
          IngestFailed(
            summary = AuditSummary.create(
              root = root,
              space = storageSpace,
              audit = AuditFailure(err),
              t = startTime
            ),
            err
          )
      }
    }

  private def getBagIdentifier(
    bagRootLocation: ObjectLocation): Try[ExternalIdentifier] =
    for {
      bagInfoLocation <- s3BagLocator.locateBagInfo(bagRootLocation)
      inputStream <- bagInfoLocation.toInputStream match {
        case Left(e)                  => Failure(e)
        case Right(None)              => Failure(StreamUnavailable("No stream available!"))
        case Right(Some(inputStream)) => Success(inputStream)
      }
      bagInfo <- BagInfo.create(inputStream)
    } yield bagInfo.externalIdentifier
}
