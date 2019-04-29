package uk.ac.wellcome.platform.storage.bagauditor.services

import java.io.InputStream
import java.time.Instant

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.common.ConvertibleToInputStream._
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagInfo,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.bagit.parsers.BagInfoParser
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestFailed,
  IngestStepResult,
  IngestStepSucceeded,
  StorageSpace
}
import uk.ac.wellcome.platform.archive.common.storage.services.S3BagLocator
import uk.ac.wellcome.platform.storage.bagauditor.models.{
  AuditInformation,
  AuditSummary
}
import uk.ac.wellcome.storage.ObjectLocation

import scala.concurrent.{ExecutionContext, Future}

class BagAuditor(implicit s3Client: AmazonS3, ec: ExecutionContext) {
  val s3BagLocator = new S3BagLocator(s3Client)

  def getAuditSummary(
    unpackLocation: ObjectLocation,
    storageSpace: StorageSpace): Future[IngestStepResult[AuditSummary]] = {
    val auditSummary = AuditSummary(
      startTime = Instant.now(),
      unpackLocation = unpackLocation,
      storageSpace = storageSpace
    )

    val auditInformation = for {
      bagRootLocation <- Future.fromTry {
        s3BagLocator.locateBagRoot(unpackLocation)
      }
      externalIdentifier <- getBagIdentifier(bagRootLocation)
      info = AuditInformation(
        bagRootLocation = bagRootLocation,
        externalIdentifier = externalIdentifier
      )
    } yield info

    auditInformation
      .map { info =>
        IngestStepSucceeded(
          auditSummary
            .copy(maybeAuditInformation = Some(info))
            .complete
        )
      }
      .recover {
        case t: Throwable =>
          IngestFailed(
            auditSummary.complete,
            t
          )
      }
  }

  private def getBagIdentifier(
    bagRootLocation: ObjectLocation): Future[ExternalIdentifier] =
    for {
      bagInfoLocation <- Future.fromTry {
        s3BagLocator.locateBagInfo(bagRootLocation)
      }
      inputStream: InputStream <- bagInfoLocation.toInputStream
      bagInfo: BagInfo <- BagInfoParser.create(inputStream)
    } yield bagInfo.externalIdentifier

}
