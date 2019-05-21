package uk.ac.wellcome.platform.archive.common.storage.services

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.storage.LocationNotFound
import uk.ac.wellcome.platform.archive.common.verify._

import scala.util.{Failure, Success}

class S3ObjectVerifier(implicit s3Client: AmazonS3)
    extends Verifier
    with Logging {

  import uk.ac.wellcome.platform.archive.common.storage.Locatable._
  import uk.ac.wellcome.platform.archive.common.storage.services.S3StreamableInstances._
  import uk.ac.wellcome.platform.archive.common.storage.services.S3LocatableInstances._

  def verify(verifiableLocation: VerifiableLocation): VerifiedLocation = {
    debug(s"S3ObjectVerifier: Attempting to verify: $verifiableLocation")

    val algorithm = verifiableLocation.checksum.algorithm

    val eitherInputStream = for {
      objectLocation <- verifiableLocation.uri.locate
      inputStream <- objectLocation.toInputStream
    } yield inputStream

    val result = eitherInputStream match {
      case Left(e) =>
        VerifiedFailure(
          verifiableLocation,
          LocationNotFound(verifiableLocation, "Failure while getting location!")
        )
      case Right(None) => VerifiedFailure(
        verifiableLocation,
        LocationNotFound(verifiableLocation, "Location not available!")
      )
      case Right(Some(inputStream)) => Checksum.create(inputStream, algorithm) match {
        case Failure(e) =>
          VerifiedFailure(verifiableLocation, FailedChecksumCreation(algorithm, e))
        case Success(checksum) => if (checksum != verifiableLocation.checksum) {
          VerifiedFailure(verifiableLocation, FailedChecksumNoMatch(checksum, verifiableLocation.checksum))
        } else {
          VerifiedSuccess(verifiableLocation)
        }
      }
    }

    debug(s"Got: $result")
    result
  }
}
