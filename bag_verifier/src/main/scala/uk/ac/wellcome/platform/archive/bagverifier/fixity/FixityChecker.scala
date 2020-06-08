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
import uk.ac.wellcome.storage.tags.Tags
import uk.ac.wellcome.storage.{DoesNotExistError, ObjectLocation}

import scala.util.{Failure, Success}

/** Look up and check the fixity info (checksum, size) on an individual file.
  *
  */
trait FixityChecker extends Logging {
  protected val streamStore: StreamStore[ObjectLocation]
  protected val tags: Tags[ObjectLocation]

  def locate(uri: URI): Either[LocateFailure[URI], ObjectLocation]

  def check(expectedFileFixity: ExpectedFileFixity): FileFixityResult = {
    debug(s"Attempting to verify: $expectedFileFixity")

    val algorithm = expectedFileFixity.checksum.algorithm

    val result: Either[FileFixityError, FileFixityCorrect] = for {
      inputStreamData <- getInputStream(expectedFileFixity)
      (inputStream, objectLocation) = inputStreamData

      _ <- verifySize(
        expectedFileFixity = expectedFileFixity,
        inputStream = inputStream,
        objectLocation = objectLocation
      )

      result <- verifyChecksum(
        expectedFileFixity = expectedFileFixity,
        inputStream = inputStream,
        objectLocation = objectLocation,
        algorithm = algorithm
      )
    } yield result

    debug(s"Got: $result")

    result match {
      case Left(fixityError)    => fixityError
      case Right(fixityCorrect) => fixityCorrect
    }
  }

  private def getInputStream(expectedFileFixity: ExpectedFileFixity):
      Either[FileFixityError, (InputStreamWithLength, ObjectLocation)] = {
    val lookupResult = for {
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

    lookupResult
      .left.map { err => FileFixityCouldNotRead(expectedFileFixity, e = err) }
  }

  private def verifySize(
    expectedFileFixity: ExpectedFileFixity,
    inputStream: InputStreamWithLength,
    objectLocation: ObjectLocation
  ): Either[FileFixityError, Unit] =
    expectedFileFixity.length match {
      case Some(expectedLength) =>
        debug(
          "Location specifies an expected length, checking it's correct"
        )

        if (expectedLength == inputStream.length) {
          Right(())
        } else {
          Left(
            FileFixityMismatch(
              expectedFileFixity = expectedFileFixity,
              objectLocation = objectLocation,
              e = new Throwable(
                "" +
                  s"Lengths do not match: $expectedLength != ${inputStream.available()}"
              )
            )
          )
        }

      case _ => Right(())
    }

  private def verifyChecksum(
    expectedFileFixity: ExpectedFileFixity,
    inputStream: InputStreamWithLength,
    objectLocation: ObjectLocation,
    algorithm: HashingAlgorithm
  ): Either[FileFixityError, FileFixityCorrect] =
    Checksum.create(inputStream, algorithm) match {
      // Failure to create a checksum (parsing/algorithm)
      case Failure(e) =>
        Left(
          FileFixityMismatch(
            expectedFileFixity = expectedFileFixity,
            objectLocation = objectLocation,
            e = FailedChecksumCreation(algorithm, e)
          )
        )

      // Checksum does not match that provided
      case Success(checksum) =>
        if (checksum != expectedFileFixity.checksum)
          Left(
            FileFixityMismatch(
              expectedFileFixity = expectedFileFixity,
              objectLocation = objectLocation,
              e = FailedChecksumNoMatch(
                actual = checksum,
                expected = expectedFileFixity.checksum
              )
            )
          )
        else
          Right(
            FileFixityCorrect(
              expectedFileFixity = expectedFileFixity,
              objectLocation = objectLocation,
              size = inputStream.length
            )
          )
    }
}
