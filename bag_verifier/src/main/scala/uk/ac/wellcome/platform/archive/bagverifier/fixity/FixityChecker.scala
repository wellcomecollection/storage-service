package uk.ac.wellcome.platform.archive.bagverifier.fixity
import java.net.URI

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.storage.{
  LocateFailure,
  LocationError,
  LocationNotFound,
  LocationParsingError
}
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

  def check(expectedFileFixity: ExpectedFileFixity): FileFixityResult = {
    debug(s"Attempting to verify: $expectedFileFixity")

    val algorithm = expectedFileFixity.checksum.algorithm

    val eitherInputStream = for {
      objectLocation <- locate(expectedFileFixity.uri) match {
        case Right(l) => Right(l)
        case Left(e) =>
          Left(
            LocationParsingError(expectedFileFixity, e.msg)
          )
      }

      inputStream <- streamStore.get(objectLocation) match {
        case Right(stream) => Right(stream.identifiedT)

        case Left(_: DoesNotExistError) =>
          Left(
            LocationNotFound(expectedFileFixity, "Location not available!")
          )

        case Left(storageError) =>
          Left(
            LocationError(expectedFileFixity, storageError.e.getMessage)
          )
      }
    } yield (inputStream, objectLocation)

    val result = eitherInputStream match {
      case Left(e) => FileFixityCouldNotRead(expectedFileFixity, e = e)

      case Right((inputStream, objectLocation)) =>
        val verifiedLocation = expectedFileFixity.length match {
          case Some(expectedLength) =>
            debug(
              "Location specifies an expected length, checking it's correct"
            )

            if (expectedLength == inputStream.length) {
              verifyChecksum(
                expectedFileFixity = expectedFileFixity,
                objectLocation = objectLocation,
                inputStream = inputStream,
                algorithm = algorithm
              )
            } else {
              FileFixityMismatch(
                expectedFileFixity = expectedFileFixity,
                objectLocation = objectLocation,
                e = new Throwable(
                  "" +
                    s"Lengths do not match: $expectedLength != ${inputStream.available()}"
                )
              )
            }

          case None =>
            verifyChecksum(
              expectedFileFixity = expectedFileFixity,
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
    expectedFileFixity: ExpectedFileFixity,
    objectLocation: ObjectLocation,
    inputStream: InputStreamWithLength,
    algorithm: HashingAlgorithm
  ): FileFixityResult =
    Checksum.create(inputStream, algorithm) match {
      // Failure to create a checksum (parsing/algorithm)
      case Failure(e) =>
        FileFixityMismatch(
          expectedFileFixity = expectedFileFixity,
          objectLocation = objectLocation,
          e = FailedChecksumCreation(algorithm, e)
        )

      // Checksum does not match that provided
      case Success(checksum) =>
        if (checksum != expectedFileFixity.checksum)
          FileFixityMismatch(
            expectedFileFixity = expectedFileFixity,
            objectLocation = objectLocation,
            e = FailedChecksumNoMatch(
              actual = checksum,
              expected = expectedFileFixity.checksum
            )
          )
        else
          FileFixityCorrect(
            expectedFileFixity = expectedFileFixity,
            objectLocation = objectLocation,
            size = inputStream.length
          )
    }
}
