package uk.ac.wellcome.platform.archive.bagverifier.services

import java.time.Instant

import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bagverifier.models.{
  FailedVerification,
  VerificationSummary
}
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagDigestFile,
  BagLocation
}
import uk.ac.wellcome.platform.archive.common.storage.models.{
  FileManifest,
  IngestFailed,
  IngestStepResult,
  IngestStepSucceeded
}
import uk.ac.wellcome.platform.archive.common.storage.services.{
  ChecksumVerifier,
  S3BagLocator,
  StorageManifestService
}
import uk.ac.wellcome.storage.ObjectLocation

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class Verifier(
  storageManifestService: StorageManifestService,
  s3Client: AmazonS3,
  algorithm: String
)(implicit ec: ExecutionContext, mat: Materializer)
    extends Logging {
  val s3BagLocator = new S3BagLocator(s3Client)

  def verify(
    bagLocation: BagLocation
  ): Future[IngestStepResult[VerificationSummary]] = {

    val verificationInit =
      VerificationSummary(bagLocation = bagLocation, startTime = Instant.now)

    val verification = for {
      bagRootLocation <- Future.fromTry {
        s3BagLocator.locateBagRoot(bagLocation.objectLocation)
      }
      fileManifest <- getManifest("file manifest") {
        storageManifestService.createFileManifest(bagRootLocation)
      }
      tagManifest <- getManifest("tag manifest") {
        storageManifestService.createTagManifest(bagRootLocation)
      }

      digestFiles = fileManifest.files ++ tagManifest.files

      result <- verifyFiles(bagRootLocation, digestFiles, verificationInit)
    } yield result

    verification.map {
      case summary if summary.succeeded => IngestStepSucceeded(summary)
      case failed =>
        IngestFailed(
          failed,
          new RuntimeException("Verification failed")
        )
    } recover {
      case e: Throwable => IngestFailed(verificationInit, e)
    }
  }

  private def getManifest(name: String)(
    result: Future[FileManifest]): Future[FileManifest] =
    result.recover {
      case err: Throwable =>
        throw new RuntimeException(s"Error getting $name: ${err.getMessage}")
    }

  private def verifyFiles(
    bagRootLocation: ObjectLocation,
    digestFiles: Seq[BagDigestFile],
    bagVerification: VerificationSummary
  )(implicit mat: Materializer): Future[VerificationSummary] = {
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

  private def verifyIndividualFile(
    bagRootLocation: ObjectLocation,
    digestFile: BagDigestFile): Either[FailedVerification, BagDigestFile] = {
    val objectLocation = digestFile.path.toObjectLocation(bagRootLocation)
    for {
      inputStream <- Try {
        s3Client
          .getObject(objectLocation.namespace, objectLocation.key)
          .getObjectContent
      }.toEither.left.map(e => {
        warn(s"Could not verify $objectLocation : $e")
        FailedVerification(digestFile = digestFile, reason = e)
      })

      actualChecksum <- ChecksumVerifier
        .checksum(
          inputStream,
          algorithm = algorithm
        )
        .toEither
        .left
        .map(e => FailedVerification(digestFile = digestFile, reason = e))

      result <- getResult(digestFile, actualChecksum = actualChecksum)
    } yield result
  }

  private def getResult(
    digestFile: BagDigestFile,
    actualChecksum: String
  ): Either[FailedVerification, BagDigestFile] =
    if (digestFile.checksum == actualChecksum) {
      Right(digestFile)
    } else {
      Left(
        FailedVerification(
          digestFile = digestFile,
          reason = new RuntimeException(
            s"Checksums do not match: expected ${digestFile.checksum}, actually saw $actualChecksum")
        ))
    }
}
