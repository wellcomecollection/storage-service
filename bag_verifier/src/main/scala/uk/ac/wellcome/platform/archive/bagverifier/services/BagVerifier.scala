package uk.ac.wellcome.platform.archive.bagverifier.services

import java.time.Instant

import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import cats.Id
import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bagverifier.models._
import uk.ac.wellcome.platform.archive.common.bagit.models._
import uk.ac.wellcome.platform.archive.common.verify.Verifiable._
import uk.ac.wellcome.platform.archive.common.verify.Verification._
import uk.ac.wellcome.platform.archive.common.storage.Resolvable._
import uk.ac.wellcome.platform.archive.common.bagit.models.BagFile
import uk.ac.wellcome.platform.archive.common.storage.models._
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestService
import uk.ac.wellcome.platform.archive.common.verify.ChecksumAlgorithm
import uk.ac.wellcome.storage.ObjectLocation

import scala.concurrent.{ExecutionContext, Future}

class BagVerifier(
  storageManifestService: StorageManifestService,
  s3Client: AmazonS3,
  algorithm: ChecksumAlgorithm
)(implicit ec: ExecutionContext, mat: Materializer, s3ObjectVerifier: S3ObjectVerifier)
    extends Logging {

  type FutureStep = Future[IngestStepResult[VerificationSummary]]

  def verify(root: ObjectLocation): FutureStep = {

    val initialSummary = VerificationSummary(root)

    val verification = for {
      (fileManifest, tagManifest) <- getManifests(root)

      verifyManifests(root)(fileManifest, tagManifest)

//      summary1 <- processManifest(root, fileManifest, initialSummary)
//      finalSummary <- processManifest(root, tagManifest, summary1)

    } yield finalSummary

    verification.map {
      case summary if summary.succeeded => IngestStepSucceeded(summary)
      case failed =>
        IngestFailed(
          failed,
          new RuntimeException("Verification failed")
        )
    } recover {
      case e: Throwable => IngestFailed(initialSummary, e)
    }
  }

  private val getManifests = (root: ObjectLocation) => for {
    fileManifest <-
      storageManifestService
        .createFileManifest(root)
    tagManifest <-
      storageManifestService
        .createTagManifest(root)
  } yield (fileManifest, tagManifest)

  private val verifyManifests = (root: ObjectLocation) =>
    (fileManifest: BagManifest, tagManifest: BagManifest) => {
      fileManifest.verify(root)(fileManifest.checksumAlgorithm)
      tagManifest.verify(root)(tagManifest.checksumAlgorithm)

      // provide summary
    }

  private val verifyFile =
    (root: ObjectLocation) =>
      (algorithm: ChecksumAlgorithm) =>
        (file: BagFile) => Future {
          file.verify(root)(algorithm)
        }


//  private def bagVerifierSink(summary: VerificationSummary) = Sink.fold(summary) { (memo: VerificationSummary, item) =>
//    item match {
//      case Left(failedVerification) =>
//        memo.copy(failedVerifications =
//          memo.failedVerifications :+ failedVerification)
//      case Right(digestFile) =>
//        memo.copy(successfulVerifications =
//          memo.successfulVerifications :+ digestFile)
//    }
//  }



//  private def processManifest(
//    root: ObjectLocation,
//    fileManifest: FileManifest,
//    bagVerification: VerificationSummary
//  ): Future[VerificationSummary] = {
//    Source[BagDigestFile](
//      fileManifest.files
//    ).mapAsync(10)(
//      verifyFile(root)(fileManifest.checksumAlgorithm)(_)
//    ).runWith(
//      bagVerifierSink
//    ).map(bagVerification => bagVerification.complete)
//  }
}
