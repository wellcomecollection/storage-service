package uk.ac.wellcome.platform.archive.bagverifier.services

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.storage.LocationNotFound
import uk.ac.wellcome.platform.archive.common.verify._

import scala.util.{Failure, Success}

class S3ObjectVerifier(implicit s3Client: AmazonS3)
    extends Verifier
    with Logging {
  import uk.ac.wellcome.platform.archive.common.storage.services.S3StreamableInstances._

  def verify(location: VerifiableLocation): VerifiedLocation = {
    debug(s"Attempting to verify: $location")

    val algorithm = location.checksum.algorithm

    val result = location.objectLocation.locate match {
      case Left(e) =>
        VerifiedFailure(location, LocationNotFound(location, "Failure while getting location"))
      case Right(None) =>
        VerifiedFailure(location, LocationNotFound(location, "Location not found"))
      case Right(Some(s)) => Checksum.create(s, algorithm) match {
        case Failure(e) =>
          VerifiedFailure(location, FailedChecksumCreation(algorithm, e))
        case Success(checksum) => if (checksum != location.checksum) {
          VerifiedFailure(location, FailedChecksumNoMatch(checksum, location.checksum))
        } else {
          VerifiedSuccess(location)
        }
      }
    }

    debug(s"Got: $result")
    result
  }
}
