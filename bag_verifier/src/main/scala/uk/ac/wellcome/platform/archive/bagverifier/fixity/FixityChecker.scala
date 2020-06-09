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

    val fixityResult = for {
      location <- parseLocation(expectedFileFixity)
      _ = debug(s"Parsed location for ${expectedFileFixity.uri} as $location")

      inputStream <- openInputStream(expectedFileFixity, location)
      _ = debug(s"Opened input stream for $location")

      _ <- verifySize(expectedFileFixity, location, inputStream)
      _ = debug(s"Checked the size of $location is correct")

      result <- verifyChecksumFromInputStream(
        expectedFileFixity = expectedFileFixity,
        location = location,
        inputStream = inputStream,
        algorithm = algorithm
      )
    } yield result

    debug(s"Fixity check result for ${expectedFileFixity.uri}: $fixityResult")

    fixityResult match {
      case Right(fixityCorrect) => fixityCorrect
      case Left(fixityError)    => fixityError
    }
  }

  private def parseLocation(
    expectedFileFixity: ExpectedFileFixity
  ): Either[FileFixityCouldNotRead, ObjectLocation] =
    locate(expectedFileFixity.uri) match {
      case Right(location) => Right(location)
      case Left(locateError) =>
        Left(
          FileFixityCouldNotRead(
            expectedFileFixity = expectedFileFixity,
            e = LocationParsingError(expectedFileFixity, locateError.msg)
          )
        )
    }

  private def openInputStream(
    expectedFileFixity: ExpectedFileFixity,
    location: ObjectLocation
  ): Either[FileFixityCouldNotRead, InputStreamWithLength] =
    streamStore.get(location) match {
      case Right(stream) => Right(stream.identifiedT)

      case Left(_: DoesNotExistError) =>
        Left(
          FileFixityCouldNotRead(
            expectedFileFixity = expectedFileFixity,
            e = LocationNotFound(expectedFileFixity, "Location not available!")
          )
        )

      case Left(readError) =>
        Left(
          FileFixityCouldNotRead(
            expectedFileFixity = expectedFileFixity,
            e = LocationError(expectedFileFixity, readError.e.getMessage)
          )
        )
    }

  private def verifySize(
    expectedFileFixity: ExpectedFileFixity,
    location: ObjectLocation,
    inputStream: InputStreamWithLength
  ): Either[FileFixityMismatch, Unit] =
    expectedFileFixity.length match {
      case Some(expectedLength) if expectedLength != inputStream.length =>
        Left(
          FileFixityMismatch(
            expectedFileFixity = expectedFileFixity,
            objectLocation = location,
            e = new Throwable(
              s"Lengths do not match: $expectedLength (expected) != ${inputStream.length} (actual)"
            )
          )
        )

      case _ => Right(())
    }

  private def verifyChecksumFromInputStream(
    expectedFileFixity: ExpectedFileFixity,
    location: ObjectLocation,
    inputStream: InputStreamWithLength,
    algorithm: HashingAlgorithm
  ): Either[FileFixityError, FileFixityCorrect] = {
    val fixityResult = Checksum.create(inputStream, algorithm) match {
      case Failure(e) =>
        Left(
          FileFixityCouldNotGetChecksum(
            expectedFileFixity = expectedFileFixity,
            objectLocation = location,
            e = FailedChecksumCreation(algorithm, e)
          )
        )

      case Success(checksum) if checksum == expectedFileFixity.checksum =>
        Right(
          FileFixityCorrect(
            expectedFileFixity = expectedFileFixity,
            objectLocation = location,
            size = inputStream.length
          )
        )

      case Success(checksum) =>
        Left(
          FileFixityMismatch(
            expectedFileFixity = expectedFileFixity,
            objectLocation = location,
            e = FailedChecksumNoMatch(
              actual = checksum,
              expected = expectedFileFixity.checksum
            )
          )
        )
    }

    // Remember to close the InputStream when we're done, whatever the result.
    // If we don't, the verifier will accumulate open streams and run out of
    // either memory or file descriptors.
    inputStream.close()

    fixityResult
  }
}
