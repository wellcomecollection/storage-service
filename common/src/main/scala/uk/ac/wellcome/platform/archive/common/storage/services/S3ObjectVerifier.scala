package uk.ac.wellcome.platform.archive.common.storage.services

import java.io.InputStream
import java.net.URI

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.storage.{LocateFailure, LocationError, LocationNotFound, LocationParsingError}
import uk.ac.wellcome.platform.archive.common.verify._
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.{DoesNotExistError, ObjectLocation}
import uk.ac.wellcome.storage.store.s3.S3StreamStore
import uk.ac.wellcome.storage.streaming.{HasLength, InputStreamWithLengthAndMetadata}

import scala.util.{Failure, Success}

trait BetterVerifier[IS <: InputStream with HasLength] extends Logging {
  protected val streamStore: StreamStore[ObjectLocation, IS]

  def locate(uri: URI): Either[LocateFailure[URI], ObjectLocation]

  def verify(verifiableLocation: VerifiableLocation): VerifiedLocation = {
    debug(s"Attempting to verify: $verifiableLocation")

    val algorithm = verifiableLocation.checksum.algorithm

    val eitherInputStream = for {
      objectLocation <- locate(verifiableLocation.uri) match {
        case Right(l) => Right(l)
        case Left(e) =>
          Left(
            LocationParsingError(verifiableLocation, e.msg)
          )
      }

      inputStream <- streamStore.get(objectLocation) match {
        case Right(stream)                => Right(Some(stream.identifiedT))
        case Left(err: DoesNotExistError) => Right(None)
        case Left(storageError) =>
          Left(
            LocationError(verifiableLocation, storageError.e.getMessage)
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

            if (expectedLength == inputStream.length) {
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

class S3ObjectVerifier(implicit s3Client: AmazonS3)
    extends Verifier
    with BetterVerifier[InputStreamWithLengthAndMetadata]
    with Logging {

  import uk.ac.wellcome.platform.archive.common.storage.Locatable._
  import uk.ac.wellcome.platform.archive.common.storage.services.S3LocatableInstances._

  override protected val streamStore: StreamStore[ObjectLocation, InputStreamWithLengthAndMetadata] =
    new S3StreamStore()

  override def locate(uri: URI): Either[LocateFailure[URI], ObjectLocation] =
    uri.locate
}
