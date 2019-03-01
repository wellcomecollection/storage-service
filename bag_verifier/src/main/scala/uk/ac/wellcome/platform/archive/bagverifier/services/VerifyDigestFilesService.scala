package uk.ac.wellcome.platform.archive.bagverifier.services

import java.time.{Duration, Instant}

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bagverifier.models.{
  BagVerification,
  FailedVerification
}
import uk.ac.wellcome.platform.archive.common.models.FileManifest
import uk.ac.wellcome.platform.archive.common.models.bagit.{
  BagDigestFile,
  BagLocation
}
import uk.ac.wellcome.platform.archive.common.services.StorageManifestService
import uk.ac.wellcome.platform.archive.common.storage.ChecksumVerifier

import scala.concurrent.{ExecutionContext, Future}

class VerifyDigestFilesService(storageManifestService: StorageManifestService,
                               s3Client: AmazonS3,
                               algorithm: String)(implicit ec: ExecutionContext)
    extends Logging {
  def verifyBag(bagLocation: BagLocation): Future[BagVerification] =
    for {
      fileManifest <- getManifest("file manifest") {
        storageManifestService.createFileManifest(bagLocation)
      }
      tagManifest <- getManifest("tag manifest") {
        storageManifestService.createTagManifest(bagLocation)
      }
      digestFiles = fileManifest.files ++ tagManifest.files
      result <- verifyFiles(bagLocation, digestFiles = digestFiles)
    } yield result

  private def getManifest(name: String)(
    result: Future[FileManifest]): Future[FileManifest] =
    result.recover {
      case err: Throwable =>
        throw new RuntimeException(s"Error getting $name: ${err.getMessage}")
    }

  private def verifyFiles(
    bagLocation: BagLocation,
    digestFiles: Seq[BagDigestFile]): Future[BagVerification] = {
    val verificationStart = Instant.now
    Future
      .traverse(digestFiles) { digestFile: BagDigestFile =>
        verifyIndividualFile(bagLocation, digestFile = digestFile)
          .recover {
            case reason: Throwable =>
              Left(
                FailedVerification(
                  digestFile = digestFile,
                  reason = reason
                ))
          }
      }
      .map { results =>
        val successfulVerifications = results.collect {
          case Right(digestFile) => digestFile
        }
        val failedVerifications = results.collect {
          case Left(failedVerification) => failedVerification
        }

        assert(
          successfulVerifications.size + failedVerifications.size == digestFiles.size)

        BagVerification(
          successfulVerifications = successfulVerifications,
          failedVerifications = failedVerifications,
          duration = Duration.between(verificationStart, Instant.now)
        )
      }
  }

  private def verifyIndividualFile(bagLocation: BagLocation,
                                   digestFile: BagDigestFile)
    : Future[Either[FailedVerification, BagDigestFile]] = {
    val objectLocation = digestFile.path.toObjectLocation(bagLocation)
    for {
      inputStream <- Future {
        s3Client
          .getObject(objectLocation.namespace, objectLocation.key)
          .getObjectContent
      }
      actualChecksum <- ChecksumVerifier.checksum(
        inputStream,
        algorithm = algorithm)
    } yield getResult(digestFile, actualChecksum)
  }

  private def getResult(
    digestFile: BagDigestFile,
    actualChecksum: String): Either[FailedVerification, BagDigestFile] =
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
