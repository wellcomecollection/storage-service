package uk.ac.wellcome.platform.archive.bagverifier.fixity
import java.net.URI

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.storage.services.SizeFinder
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

import scala.util.{Failure, Success, Try}

/** Look up and check the fixity info (checksum, size) on an individual file.
  *
  */
trait FixityChecker extends Logging {
  protected val streamStore: StreamStore[ObjectLocation]
  protected val tags: Tags[ObjectLocation]
  protected val sizeFinder: SizeFinder

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
    val fixityTagName = s"Content-$algorithm"
    val fixityTagValue = expectedFileFixity.checksum.value.toString
    val fixityTags = Map(fixityTagName -> fixityTagValue)

    val result: Either[FileFixityError, FileFixityCorrect] = for {
      objectLocation <- getObjectLocation(expectedFileFixity)
      _ = debug(s"ObjectLocation for URI ${expectedFileFixity.uri} is $objectLocation")

      existingTags <- tags.get(objectLocation) match {
        case Right(tagSet)   => Right(tagSet)
        case Left(readError) => Left(
          FileFixityCouldNotRead(expectedFileFixity, e = readError.e)
        )
      }
      _ = debug(s"Existing tags on $objectLocation: $existingTags")

      size <- verifySize(
        expectedFileFixity = expectedFileFixity,
        objectLocation = objectLocation
      )

      checksumResult <- verifyChecksum(
        expectedFileFixity = expectedFileFixity,
        existingTags = existingTags,
        fixityTagName = fixityTagName,
        fixityTagValue = fixityTagValue,
        objectLocation = objectLocation,
        algorithm = algorithm,
        size = size
      )

      _ = debug(s"@@AWLC checksum result == $checksumResult")

      _ <- writeChecksumTag(
        expectedFileFixity = expectedFileFixity,
        objectLocation = objectLocation,
        fixityTags = fixityTags
      )
    } yield checksumResult

    debug(s"Got: $result")

    result match {
      case Left(fixityError)    => fixityError
      case Right(fixityCorrect) => fixityCorrect
    }
  }

  private def getObjectLocation(expectedFileFixity: ExpectedFileFixity): Either[FileFixityCouldNotRead, ObjectLocation] =
    locate(expectedFileFixity.uri)
      .left.map { locateFailure =>
        FileFixityCouldNotRead(
          expectedFileFixity = expectedFileFixity,
          e = LocationParsingError(expectedFileFixity, locateFailure.msg)
        )
      }

  private def getInputStream(objectLocation: ObjectLocation, expectedFileFixity: ExpectedFileFixity):
      Either[FileFixityError, InputStreamWithLength] = {
    val lookupResult = for {
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
    } yield inputStream

    lookupResult
      .left.map { err => FileFixityCouldNotRead(expectedFileFixity, e = err) }
  }

  private def verifySize(
    expectedFileFixity: ExpectedFileFixity,
    objectLocation: ObjectLocation
  ): Either[FileFixityError, Long] =
    for {
      actualLength <- sizeFinder.getSize(objectLocation) match {
        case Success(size) => Right(size)
        case Failure(_)  => Left(
          FileFixityCouldNotRead(
            expectedFileFixity,
            e = LocationNotFound(expectedFileFixity, "Location not available!")
          )
        )
      }

      _ <- expectedFileFixity.length match {
        case Some(expectedLength) if expectedLength != actualLength =>
          Left(
            FileFixityMismatch(
              expectedFileFixity = expectedFileFixity,
              objectLocation = objectLocation,
              e = new Throwable(
                s"Lengths do not match: $expectedLength (expected) != $actualLength (actual)"
              )
            )
          )

        case _ => Right(())
      }
    } yield actualLength

  private def verifyChecksum(
    expectedFileFixity: ExpectedFileFixity,
    objectLocation: ObjectLocation,
    existingTags: Map[String, String],
    fixityTagName: String,
    fixityTagValue: String,
    algorithm: HashingAlgorithm,
    size: Long
  ): Either[FileFixityError, FileFixityCorrect] =
    // We start by looking for an existing fixity tag on the object: if it's
    // already there, we can skip reading the entire object a second time.
    existingTags.get(fixityTagName) match {
      case None =>
        verifyChecksumByReadingObject(
          expectedFileFixity = expectedFileFixity,
          objectLocation = objectLocation,
          algorithm = algorithm,
          size = size
        )

      case Some(storedValue) if storedValue == fixityTagValue => {
        debug(s"@@AWLC boo wrong branch in verifyChecksum(!")
        Right(
          FileFixityCorrect(
            expectedFileFixity = expectedFileFixity,
            objectLocation = objectLocation,
            size = size
          )
        )
      }

      case Some(storedValue) =>
        Left(
          FileFixityMismatch(
            expectedFileFixity = expectedFileFixity,
            objectLocation = objectLocation,
            e = new Throwable(
              s"Checksum mismatch: object tags ($storedValue) does not match manifest checksum (${expectedFileFixity.checksum})"
            )
          )
        )
    }

  private def verifyChecksumByReadingObject(
    expectedFileFixity: ExpectedFileFixity,
    objectLocation: ObjectLocation,
    algorithm: HashingAlgorithm,
    size: Long
  ): Either[FileFixityError, FileFixityCorrect] =
    for {
      inputStream <- getInputStream(objectLocation, expectedFileFixity)

      result <- Checksum.create(inputStream, algorithm) match {
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
          if (checksum != expectedFileFixity.checksum) {
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
          }

          // Checksum is correct
          else {
            assert(inputStream.length == size)
            Right(
              FileFixityCorrect(
                expectedFileFixity = expectedFileFixity,
                objectLocation = objectLocation,
                size = inputStream.length
              )
            )
          }
      }
    } yield result

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
          } match {
            case Right(_)          => Right(())
            case Left(updateError) => Left(
              FileFixityCouldNotWriteTag(
                expectedFileFixity = expectedFileFixity,
                objectLocation = objectLocation,
                e = updateError.e
              )
            )
          }
      } match {
        case Success(result) => result
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
