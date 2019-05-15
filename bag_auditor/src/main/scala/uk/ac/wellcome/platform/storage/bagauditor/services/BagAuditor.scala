package uk.ac.wellcome.platform.storage.bagauditor.services

import java.time.Instant

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagInfo,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.IngestID
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

  def getAuditSummary(
    ingestId: IngestID,
    ingestDate: Instant,
    unpackLocation: ObjectLocation,
    storageSpace: StorageSpace): IngestStep =
    Try {
      val startTime = Instant.now()

      val auditTry: Try[AuditSuccess] = for {
        root <- s3BagLocator.locateBagRoot(unpackLocation)
        externalIdentifier <- getBagIdentifier(root)
        version <- versionPicker.chooseVersion(
          externalIdentifier = externalIdentifier,
          ingestId = ingestId,
          ingestDate = ingestDate
        )
        auditSuccess = AuditSuccess(
          root = root,
          externalIdentifier = externalIdentifier,
          version = version
        )
      } yield auditSuccess

      auditTry recover {
        case e => AuditFailure(e)
      } match {
        case Success(audit @ AuditSuccess(_, _, _)) =>
          IngestStepSucceeded(
            AuditSummary.create(
              location = unpackLocation,
              space = storageSpace,
              audit = audit,
              t = startTime
            )
          )
        case Success(audit @ AuditFailure(e)) =>
          IngestFailed(
            summary = AuditSummary.create(
              location = unpackLocation,
              space = storageSpace,
              audit = audit,
              t = startTime
            ),
            e
          )
        case Failure(e) =>
          IngestFailed(
            summary = AuditSummary.incomplete(
              location = unpackLocation,
              space = storageSpace,
              e = e,
              t = startTime
            ),
            e
          )
      }
    }

  private def getBagIdentifier(
    bagRootLocation: ObjectLocation): Try[ExternalIdentifier] =
    for {
      bagInfoLocation <- s3BagLocator.locateBagInfo(bagRootLocation)
      inputStream <- bagInfoLocation.toInputStream
      bagInfo <- BagInfo.create(inputStream)
    } yield bagInfo.externalIdentifier
}
