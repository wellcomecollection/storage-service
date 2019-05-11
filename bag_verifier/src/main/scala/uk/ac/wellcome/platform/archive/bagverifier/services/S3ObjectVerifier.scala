package uk.ac.wellcome.platform.archive.bagverifier.services

import cats.Id
import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.storage.services.ChecksumVerifier
import uk.ac.wellcome.platform.archive.common.verify._

import scala.util.Try

class S3ObjectVerifier(s3Client: AmazonS3) extends LocationVerifier with Logging {
  def verify(location: VerifiableLocation)
  : Either[VerificationFailure[Id], Unit] =
    for {
      inputStream <- toEither(location) {
        Try {
          s3Client
            .getObject(
              location.objectLocation.namespace,
              location.objectLocation.key)
            .getObjectContent
        }
      }

      actualChecksum <- toEither(location) {
        ChecksumVerifier
          .checksum(
            inputStream,
            algorithm = location.checksum.algorithm
          )
      }

      _ <- getResult(location, actualChecksum)
    } yield ()

  def toEither[T](location: VerifiableLocation)(result: => Try[T]): Either[VerificationFailure[Id], T] =
    result.toEither.left.map { error =>
      warn(s"Could not verify ${location.objectLocation}: $error")
      VerificationFailure[Id](
        VerifiableObjectLocationFailure(location, error)
      )
    }

  def getResult(location: VerifiableLocation, actualChecksum: ChecksumValue): Either[VerificationFailure[Id], Unit] =
    if (location.checksum.value == actualChecksum) {
      Right(())
    } else {
      Left(
        VerificationFailure[Id](
          VerifiableObjectLocationFailure(
            location,
            new RuntimeException(
              s"Checksums do not match: expected ${location.checksum.value}, actually saw $actualChecksum"
            )
          )
        )
      )
    }
}
