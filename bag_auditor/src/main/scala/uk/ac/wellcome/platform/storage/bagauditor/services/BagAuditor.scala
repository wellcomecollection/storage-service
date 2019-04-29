package uk.ac.wellcome.platform.storage.bagauditor.services

import java.time.Instant

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestFailed,
  IngestStepResult,
  IngestStepSucceeded,
  StorageSpace
}
import uk.ac.wellcome.platform.archive.common.storage.services.S3BagLocator
import uk.ac.wellcome.platform.storage.bagauditor.models.{AuditInformation, AuditSummary}
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.{Failure, Success, Try}

class BagAuditor(implicit s3Client: AmazonS3) {
  val s3BagLocator = new S3BagLocator(s3Client)

  def getAuditSummary(
    unpackLocation: ObjectLocation,
    storageSpace: StorageSpace): Try[IngestStepResult[AuditSummary]] = {
    val auditSummary = AuditSummary(
      startTime = Instant.now(),
      unpackLocation = unpackLocation,
      storageSpace = storageSpace
    )

    buildAuditInformation(unpackLocation) match {
      case Success(info) =>
        Success(
          IngestStepSucceeded(
            auditSummary
              .copy(maybeAuditInformation = Some(info))
              .complete
          )
        )
      case Failure(e) =>
        Success(
          IngestFailed(
            auditSummary.complete,
            e
          )
        )
    }
  }

  private def buildAuditInformation(unpackLocation: ObjectLocation): Try[AuditInformation] = {
    for {
      bagRootLocation <- s3BagLocator.locateBagRoot(unpackLocation)
    } yield AuditInformation(bagRootLocation)
  }
}
