package uk.ac.wellcome.platform.archive.bagverifier.services

import java.security.MessageDigest

import akka.Done
import com.amazonaws.services.s3.AmazonS3
import org.apache.commons.codec.digest.MessageDigestAlgorithms
import uk.ac.wellcome.messaging.sqs.NotificationStream
import uk.ac.wellcome.platform.archive.common.models.BagRequest
import uk.ac.wellcome.platform.archive.common.models.bagit.{BagDigestFile, BagLocation}
import uk.ac.wellcome.platform.archive.common.services.StorageManifestService
import uk.ac.wellcome.platform.archive.common.storage.ChecksumVerifier
import uk.ac.wellcome.typesafe.Runnable

import scala.collection.immutable
import scala.concurrent.Future

class BagVerifierWorkerService(
  notificationStream: NotificationStream[BagRequest],
  storageManifestService: StorageManifestService,
  s3Client: AmazonS3
) extends Runnable {

  val algorithm: String = MessageDigestAlgorithms.SHA_256

  def run(): Future[Done] =
    notificationStream.run(processMessage)

  def processMessage(bagRequest: BagRequest): Future[Unit] =
    for {
      manifest <- storageManifestService.createManifest(bagRequest.bagLocation)
      tagManifest <- storageManifestService.createTagManifest(bagRequest.bagLocation)
      digestFiles = manifest.manifest.files ++ tagManifest.files
      _ <- verifyFiles(bagRequest.bagLocation, digestFiles = digestFiles)
    } yield ()

  private def verifyFiles(bagLocation: BagLocation, digestFiles: Seq[BagDigestFile]): Future[Unit] =
    Future.traverse(digestFiles) {
      digestFile: BagDigestFile =>
        verifyIndividualFile(bagLocation, digestFile = digestFile)
    }.map { _ => () }

  private def verifyIndividualFile(bagLocation: BagLocation, digestFile: BagDigestFile): Future[Unit] = {
    val expectedChecksum = digestFile.checksum

    val objectLocation = digestFile.path.toObjectLocation(bagLocation)

    for {
      inputStream <- Future {
        s3Client
          .getObject(objectLocation.namespace, objectLocation.key)
          .getObjectContent
      }
      actualChecksum <- ChecksumVerifier.checksum(inputStream, algorithm = algorithm)
      checksumIsCorrect = expectedChecksum == actualChecksum
      _ <- if (!checksumIsCorrect) {
        throw new RuntimeException(s"Incorrect checksum for $digestFile; read $actualChecksum, expected $expectedChecksum")
      }
    } yield ()
  }
}