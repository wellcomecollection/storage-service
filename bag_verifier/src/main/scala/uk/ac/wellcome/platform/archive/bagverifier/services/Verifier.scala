package uk.ac.wellcome.platform.archive.bagverifier.services

import java.time.Instant

import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bagverifier.models._
import uk.ac.wellcome.platform.archive.common.bagit.models.BagDigestFile
import uk.ac.wellcome.platform.archive.common.storage.models.{
  FileManifest,
  IngestFailed,
  IngestStepResult,
  IngestStepSucceeded
}
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestService
import uk.ac.wellcome.storage.ObjectLocation

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class Verifier(
  storageManifestService: StorageManifestService,
  s3Client: AmazonS3,
  algorithm: String
)(implicit ec: ExecutionContext, materializer: Materializer)
    extends Logging {

  val objectVerifier = new ObjectVerifier(s3Client)

  def verify(
    bagRootLocation: ObjectLocation
  ): Future[IngestStepResult[VerificationSummary]] = {

    val verificationSummary = VerificationSummary(
      bagRootLocation = bagRootLocation,
      startTime = Instant.now
    )

    val verification = for {
      fileManifest <- getManifest("file manifest") {
        storageManifestService.createFileManifest(bagRootLocation)
      }
      tagManifest <- getManifest("tag manifest") {
        storageManifestService.createTagManifest(bagRootLocation)
      }

      digestFiles = fileManifest.files ++ tagManifest.files

      result <- verifyFiles(bagRootLocation, digestFiles, verificationSummary)
    } yield result

    verification.map {
      case summary if summary.succeeded => IngestStepSucceeded(summary)
      case failed =>
        IngestFailed(
          failed,
          new RuntimeException("Verification failed")
        )
    } recover {
      case e: Throwable => IngestFailed(verificationSummary, e)
    }
  }

  private def getManifest(name: String)(
    result: Try[FileManifest]): Future[FileManifest] =
    Future.fromTry(result).recover {
      case err: Throwable =>
        throw new RuntimeException(s"Error getting $name: ${err.getMessage}")
    }

  private def verifyFiles(
    bagRootLocation: ObjectLocation,
    digestFiles: Seq[BagDigestFile],
    bagVerification: VerificationSummary
  ): Future[VerificationSummary] = {
    Source[BagDigestFile](
      digestFiles.toList
    ).mapAsync(10) { digestFile: BagDigestFile =>
        Future(verifyIndividualFile(bagRootLocation, digestFile = digestFile))
      }
      .runWith(Sink.fold(bagVerification) { (memo, item) =>
        item match {
          case Left(failedVerification) =>
            memo.copy(failedVerifications =
              memo.failedVerifications :+ failedVerification)
          case Right(digestFile) =>
            memo.copy(successfulVerifications =
              memo.successfulVerifications :+ digestFile)
        }
      })
      .map(bagVerification => bagVerification.complete)
  }

  private def verifyIndividualFile(bagRootLocation: ObjectLocation,
                                   digestFile: BagDigestFile)
    : Either[FailedVerification, VerificationRequest] = {
    val verificationRequest = VerificationRequest(
      objectLocation = digestFile.path.toObjectLocation(bagRootLocation),
      checksum = Checksum(
        algorithm = algorithm,
        value = digestFile.checksum
      )
    )

    objectVerifier.verify(verificationRequest)
  }
}
