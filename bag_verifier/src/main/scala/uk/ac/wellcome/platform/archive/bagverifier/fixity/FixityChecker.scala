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
import uk.ac.wellcome.storage.{DoesNotExistError, ObjectLocation, UpdateError}

import scala.util.{Failure, Success, Try}

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

    // The FixityChecker writes a Tag to the location once it verifies the file there.
    // If it tries to re-verify a location and finds the tag is present, it
    // will skip re-reading the file.
    //
    // This means re-verifying the same content (e.g. a new version of a bag
    // that refers back to files in an old version):
    //
    //    1.  Is faster than reading from scratch
    //    2.  Allows us to cycle objects to Glacier/cold storage tiers if they're very
    //        large, and we don't need to unfreeze them to verify new versions of the bag
    //
    val fixityTags = Map(s"Content-$algorithm" -> expectedFileFixity.checksum.value.toString)

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

      _ <- writeChecksumTag(
        expectedFileFixity = expectedFileFixity,
        objectLocation = objectLocation,
        fixityTags = fixityTags
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

  private def writeChecksumTag(
    expectedFileFixity: ExpectedFileFixity,
    objectLocation: ObjectLocation,
    fixityTags: Map[String, String]): Either[FileFixityCouldNotWriteTag, Unit] =
      Try {
        debug(s"Adding tags $fixityTags to $objectLocation")
        tags
          .update(objectLocation) { existingTags =>
            // We've already checked the tags on this object once, so we shouldn't
            // see conflicting values here.  Check we're not about to blat the tags
            // just in case.
            fixityTags.foreach { case (key, value) =>
              assert(
                existingTags.getOrElse(key, value) == value,
                s"Trying to write $fixityTags to $objectLocation; existing tags conflict: $existingTags"
              )
            }

            Right(existingTags ++ fixityTags)
          }
          .left.map { updateError: UpdateError =>
            FileFixityCouldNotWriteTag(
              expectedFileFixity = expectedFileFixity,
              objectLocation = objectLocation,
              e = updateError.e
            )
          }
      } match {
        case Success(_) => Right(())
        case Failure(err) =>
          Left(
            FileFixityCouldNotWriteTag(
              expectedFileFixity = expectedFileFixity,
              objectLocation = objectLocation,
              e = err
            )
          )
      }
}
