package uk.ac.wellcome.platform.archive.bagverifier.services

import java.io.InputStream

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.common.models.bagit.{BagDigestFile, BagLocation}
import uk.ac.wellcome.platform.archive.common.services.StorageManifestService
import uk.ac.wellcome.platform.archive.common.storage.ChecksumVerifier

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

case class FailedVerification(
  digestFile: BagDigestFile,
  reason: Throwable
)

case class BagVerification(
  woke: Seq[BagDigestFile],
  problematicFaves: Seq[FailedVerification]
)

class VerifyDigestFilesService(storageManifestService: StorageManifestService, s3Client: AmazonS3, algorithm: String)(implicit ec: ExecutionContext) {
  def verifyBagLocation(bagLocation: BagLocation): Future[BagVerification] =
    for {
      manifest <- storageManifestService.createManifest(bagLocation)
      tagManifest <- storageManifestService.createTagManifest(bagLocation)
      digestFiles = manifest.manifest.files ++ tagManifest.files
      result <- verifyFiles(bagLocation, digestFiles = digestFiles)
    } yield result

  private def verifyFiles(bagLocation: BagLocation, digestFiles: Seq[BagDigestFile]): Future[BagVerification] =
    Future.traverse(digestFiles) {
      digestFile: BagDigestFile =>
        verifyIndividualFile(bagLocation, digestFile = digestFile)
    }.map { results: Seq[Either[FailedVerification, BagDigestFile]] =>
      val woke = results.collect { case Right(digestFile) => digestFile }
      val problematicFaves = results.collect { case Left(failedVerification) => failedVerification }

      BagVerification(
        woke = woke,
        problematicFaves = problematicFaves
      )
    }

  private def verifyIndividualFile(bagLocation: BagLocation, digestFile: BagDigestFile): Future[Either[FailedVerification, BagDigestFile]] = {
    val objectLocation = digestFile.path.toObjectLocation(bagLocation)

    Future {
      Try {
        s3Client
          .getObject(objectLocation.namespace, objectLocation.key)
          .getObjectContent
      }
    }.flatMap { tryInputStream: Try[InputStream] => verifyInputStream(digestFile, tryInputStream = tryInputStream) }
  }

  private def verifyInputStream(digestFile: BagDigestFile, tryInputStream: Try[InputStream]): Future[Either[FailedVerification, BagDigestFile]] =
    tryInputStream match {
      case Failure(reason) => Future.successful {
        Left(
          FailedVerification(
            digestFile = digestFile,
            reason = reason
          )
        )
      }
      case Success(inputStream) =>
        for {
          actualChecksum <- ChecksumVerifier.checksum(inputStream, algorithm = algorithm)
        } yield getResult(digestFile, actualChecksum = actualChecksum)
    }

  private def getResult(digestFile: BagDigestFile, actualChecksum: String): Either[FailedVerification, BagDigestFile] =
    if (digestFile.checksum == actualChecksum) {
      Right(digestFile)
    } else {
      Left(FailedVerification(
        digestFile = digestFile,
        reason = new RuntimeException(s"Checksums do not match: expected ${digestFile.checksum}, actually saw $actualChecksum")
      ))
    }
}
