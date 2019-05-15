package uk.ac.wellcome.platform.storage.bagauditor.services

import java.time.Instant

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.storage.bagauditor.models._
import uk.ac.wellcome.platform.archive.common.bagit.models._
import uk.ac.wellcome.platform.archive.common.storage.models._
import uk.ac.wellcome.platform.archive.common.storage.services.S3BagLocator
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.{Failure, Success, Try}

class BagAuditor(implicit s3Client: AmazonS3) {
  val s3BagLocator = new S3BagLocator(s3Client)
  import uk.ac.wellcome.platform.archive.common.storage.services.S3StreamableInstances._

  type IngestStep = Try[IngestStepResult[AuditSummary]]

  def getAuditSummary(location: ObjectLocation, space: StorageSpace): IngestStep = Try {

    val startTime = Instant.now()

    val auditTry = for {
      root <- s3BagLocator.locateBagRoot(location)
      externalIdentifier <- getBagIdentifier(root)
      version <- chooseVersion(externalIdentifier)
    } yield AuditSuccess(root, externalIdentifier, version)

    auditTry recover {
      case e => AuditFailure(e)
    } match {
      case Success(audit@AuditSuccess(_,_,_)) =>
        IngestStepSucceeded(
          AuditSummary.create(location, space, audit, startTime)
        )
      case Success(audit@AuditFailure(e)) =>
        IngestFailed(
          AuditSummary.create(location, space, audit, startTime),
          e
        )
      case Failure(e) =>
        IngestFailed(
          AuditSummary.incomplete(location, space, e, startTime),
          e
        )
    }
  }

  private def chooseVersion(externalIdentifier: ExternalIdentifier) = Success(1)

  private def getBagIdentifier(root: ObjectLocation): Try[ExternalIdentifier] =
    for {
      location <- s3BagLocator.locateBagInfo(root)
      inputStream <- location.toInputStream
      bagInfo <- BagInfo.create(inputStream)
    } yield bagInfo.externalIdentifier
}
