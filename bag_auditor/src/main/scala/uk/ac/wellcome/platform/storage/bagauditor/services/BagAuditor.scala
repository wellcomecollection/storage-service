package uk.ac.wellcome.platform.storage.bagauditor.services

import java.time.Instant

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.common.bagit.models.BagLocation
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestFailed,
  IngestStepResult,
  IngestStepSucceeded
}
import uk.ac.wellcome.platform.archive.common.storage.services.S3BagLocator
import uk.ac.wellcome.platform.storage.bagauditor.models.AuditSummary

import scala.util.{Failure, Success, Try}

class BagAuditor(implicit s3Client: AmazonS3) {
  val s3BagLocator = new S3BagLocator(s3Client)

  def locateBagRoot(
    bagLocation: BagLocation): Try[IngestStepResult[AuditSummary]] = {
    val auditSummary = AuditSummary(
      startTime = Instant.now(),
      source = bagLocation
    )

    s3BagLocator.locateBagRoot(bagLocation.objectLocation) match {
      case Success(root) =>
        Success(
          IngestStepSucceeded(
            auditSummary
              .copy(maybeRoot = Some(root))
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
}
