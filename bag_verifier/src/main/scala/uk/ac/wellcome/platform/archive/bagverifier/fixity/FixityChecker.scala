package uk.ac.wellcome.platform.archive.bagverifier.fixity
import java.net.URI

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.storage.{LocateFailure, LocationError, LocationNotFound, LocationParsingError}
import uk.ac.wellcome.platform.archive.common.verify._
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.streaming.InputStreamWithLength
import uk.ac.wellcome.storage.{DoesNotExistError, ObjectLocation}

import scala.util.{Failure, Success}

/** Look up and check the fixity info (checksum, size) on an individual file.
  *
  */
trait FixityChecker extends Logging {
  protected val streamStore: StreamStore[ObjectLocation]

  def locate(uri: URI): Either[LocateFailure[URI], ObjectLocation]

  def verify(verifiableLocation: VerifiableLocation): FixityResult = {
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
        case Right(stream) => Right(stream.identifiedT)

        case Left(_: DoesNotExistError) =>
          Left(
            LocationNotFound(verifiableLocation, "Location not available!")
          )

        case Left(storageError) =>
          Left(
            LocationError(verifiableLocation, storageError.e.getMessage)
          )
      }
    } yield (inputStream, objectLocation)

    val result = eitherInputStream match {
      case Left(e) => FixityCouldNotRead(verifiableLocation, e = e)

      case Right((inputStream, objectLocation)) =>
        val verifiedLocation = verifiableLocation.length match {
          case Some(expectedLength) =>
            debug(
              "Location specifies an expected length, checking it's correct"
            )

            if (expectedLength == inputStream.length) {
              verifyChecksum(
                verifiableLocation = verifiableLocation,
                objectLocation = objectLocation,
                inputStream = inputStream,
                algorithm = algorithm
              )
            } else {
              FixityMismatch(
                verifiableLocation = verifiableLocation,
                objectLocation = objectLocation,
                e = new Throwable(
                  "" +
                    s"Lengths do not match: $expectedLength != ${inputStream.available()}"
                )
              )
            }

          case None =>
            verifyChecksum(
              verifiableLocation = verifiableLocation,
              objectLocation = objectLocation,
              inputStream = inputStream,
              algorithm = algorithm
            )
        }

        inputStream.close()

        verifiedLocation
    }

    debug(s"Got: $result")
    result
  }

  private def verifyChecksum(
    verifiableLocation: VerifiableLocation,
    objectLocation: ObjectLocation,
    inputStream: InputStreamWithLength,
    algorithm: HashingAlgorithm
  ): FixityResult =
    Checksum.create(inputStream, algorithm) match {
      // Failure to create a checksum (parsing/algorithm)
      case Failure(e) =>
        FixityMismatch(
          verifiableLocation = verifiableLocation,
          objectLocation = objectLocation,
          e = FailedChecksumCreation(algorithm, e)
        )

      // Checksum does not match that provided
      case Success(checksum) =>
        if (checksum != verifiableLocation.checksum)
          FixityMismatch(
            verifiableLocation = verifiableLocation,
            objectLocation = objectLocation,
            e = FailedChecksumNoMatch(
              actual = checksum,
              expected = verifiableLocation.checksum
            )
          )
        else
          FixityCorrect(
            verifiableLocation = verifiableLocation,
            objectLocation = objectLocation,
            size = inputStream.length
          )
    }
}
