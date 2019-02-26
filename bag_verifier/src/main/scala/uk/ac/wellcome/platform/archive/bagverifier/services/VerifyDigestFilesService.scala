package uk.ac.wellcome.platform.archive.bagverifier.services

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.common.models.bagit.{BagDigestFile, BagLocation}
import uk.ac.wellcome.platform.archive.common.services.StorageManifestService
import uk.ac.wellcome.platform.archive.common.storage.ChecksumVerifier

import scala.concurrent.{ExecutionContext, Future}

case class FailedVerification(
  digestFile: BagDigestFile,
  actualChecksum: String
) {
  def expectedChecksum: String = digestFile.checksum
}

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

    for {
      inputStream <- Future {
        s3Client
          .getObject(objectLocation.namespace, objectLocation.key)
          .getObjectContent
      }
      actualChecksum <- ChecksumVerifier.checksum(inputStream, algorithm = algorithm)
    } yield getResult(digestFile, actualChecksum = actualChecksum)
  }

  private def getResult(digestFile: BagDigestFile, actualChecksum: String): Either[FailedVerification, BagDigestFile] =
    if (digestFile.checksum == actualChecksum) {
      Right(digestFile)
    } else {
      Left(FailedVerification(
        digestFile = digestFile,
        actualChecksum = actualChecksum
      ))
    }
}
