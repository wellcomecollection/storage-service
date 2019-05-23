package uk.ac.wellcome.platform.archive.common.storage.services

import java.io.InputStream

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.storage.{
  LocationError,
  LocationNotFound,
  LocationParsingError
}
import uk.ac.wellcome.platform.archive.common.verify._

import scala.util.{Failure, Success}

class S3ObjectVerifier(implicit s3Client: AmazonS3)
    extends Verifier
    with Logging {

  import uk.ac.wellcome.platform.archive.common.storage.Locatable._
  import uk.ac.wellcome.platform.archive.common.storage.services.S3LocatableInstances._
  import uk.ac.wellcome.platform.archive.common.storage.services.S3StreamableInstances._

  def verify(verifiableLocation: VerifiableLocation): VerifiedLocation = {
    debug(s"S3ObjectVerifier: Attempting to verify: $verifiableLocation")

    val algorithm = verifiableLocation.checksum.algorithm

    val eitherInputStream = for {
      // URI is not a valid S3 Location
      objectLocation <- verifiableLocation.uri.locate match {
        case Right(l) => Right(l)
        case Left(e) =>
          Left(
            LocationParsingError(verifiableLocation, e.msg)
          )
      }

      // ObjectLocation cannot be converted to an InputStream
      inputStream <- objectLocation.toInputStream match {
        case Right(l) => Right(l)
        case Left(e) =>
          Left(
            LocationError(verifiableLocation, e.msg)
          )
      }
    } yield inputStream

    val result = eitherInputStream match {
      case Left(e) => VerifiedFailure(verifiableLocation, e)

      // ObjectLocation was not available to retrieve (permissions/missing)
      case Right(None) =>
        VerifiedFailure(
          verifiableLocation,
          LocationNotFound(verifiableLocation, "Location not available!")
        )

      case Right(Some(inputStream)) =>
        verifiableLocation.length match {
          case Some(expectedLength) =>
            debug(
              "Location specifies an expected length, checking it's correct")

            // Note: I'm assuming that `.available()` returns the correct
            // content-length for an S3 object input stream, even though I
            // can't find mention of it anywhere in the docs...
            //
            // The alternative is getting an S3Object directly with `getObject()`
            // and inspecting the Content-Length, but that's significantly
            // more fiddly and bypasses our nice Streamable type classes.
            //
            // I'm going to leave this as-is for now, but we might need to
            // revisit it if we start getting spurious verification failures.
            //
            if (expectedLength == inputStream.available()) {
              verifyChecksum(
                verifiableLocation = verifiableLocation,
                inputStream = inputStream,
                algorithm = algorithm
              )
            } else {
              VerifiedFailure(
                verifiableLocation,
                new Throwable("" +
                  s"Lengths do not match: $expectedLength != ${inputStream.available()}")
              )
            }

          case None =>
            verifyChecksum(
              verifiableLocation = verifiableLocation,
              inputStream = inputStream,
              algorithm = algorithm
            )
        }
    }

    debug(s"Got: $result")
    result
  }

  private def verifyChecksum(verifiableLocation: VerifiableLocation,
                             inputStream: InputStream,
                             algorithm: HashingAlgorithm): VerifiedLocation =
    Checksum.create(inputStream, algorithm) match {
      // Failure to create a checksum (parsing/algorithm)
      case Failure(e) =>
        VerifiedFailure(
          verifiableLocation,
          FailedChecksumCreation(algorithm, e))

      // Checksum does not match that provided
      case Success(checksum) =>
        if (checksum != verifiableLocation.checksum) {
          VerifiedFailure(
            verifiableLocation,
            FailedChecksumNoMatch(
              checksum,
              verifiableLocation.checksum
            )
          )
        } else {
          // Happy path!
          VerifiedSuccess(verifiableLocation)
        }
    }
}
