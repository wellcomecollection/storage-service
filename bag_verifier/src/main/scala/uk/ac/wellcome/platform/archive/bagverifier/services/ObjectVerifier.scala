package uk.ac.wellcome.platform.archive.bagverifier.services

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bagverifier.models.{
  BetterFailedVerification,
  VerificationRequest
}
import uk.ac.wellcome.platform.archive.common.storage.services.ChecksumVerifier

import scala.util.Try

class ObjectVerifier(s3Client: AmazonS3) extends Logging {
  def verify(request: VerificationRequest): Either[BetterFailedVerification, VerificationRequest] =
    for {
      inputStream <- toEither(request) {
        Try {
          s3Client
            .getObject(request.objectLocation.namespace, request.objectLocation.key)
            .getObjectContent
        }
      }

      actualChecksum <- toEither(request) {
        ChecksumVerifier
          .checksum(
            inputStream,
            algorithm = request.checksum.algorithm
          )
      }

      result <- getResult(request, actualChecksum)
    } yield result

  def toEither[T](request: VerificationRequest)(result: => Try[T]): Either[BetterFailedVerification, T] =
    result.toEither.left.map { error =>
      warn(s"Could not verify ${request.objectLocation}: $error")
      BetterFailedVerification(request = request, error = error)
    }

  def getResult(request: VerificationRequest, actualChecksum: String): Either[BetterFailedVerification, VerificationRequest] =
    if (request.checksum.value == actualChecksum) {
      Right(request)
    } else {
      Left(
        BetterFailedVerification(
          request = request,
          error = new RuntimeException(
            s"Checksums do not match: expected ${request.checksum.value}, actually saw $actualChecksum"
          )
        )
      )
    }
}
