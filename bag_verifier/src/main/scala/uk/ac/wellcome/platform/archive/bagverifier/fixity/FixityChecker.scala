package uk.ac.wellcome.platform.archive.bagverifier.fixity

import java.net.URI

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bagverifier.storage.Locatable._
import uk.ac.wellcome.platform.archive.bagverifier.storage.{
  Locatable,
  LocationError,
  LocationNotFound,
  LocationParsingError
}
import uk.ac.wellcome.platform.archive.common.storage.services.SizeFinder
import uk.ac.wellcome.platform.archive.common.verify._
import uk.ac.wellcome.storage.store.Readable
import uk.ac.wellcome.storage.streaming.InputStreamWithLength
import uk.ac.wellcome.storage.tags.Tags
import uk.ac.wellcome.storage.{DoesNotExistError, Location, Prefix, ReadError}

import scala.util.{Failure, Success}

/** Look up and check the fixity info (checksum, size) on an individual file.
  *
  */
trait FixityChecker[BagLocation <: Location, BagPrefix <: Prefix[BagLocation]]
    extends Logging {
  protected val streamReader: Readable[BagLocation, InputStreamWithLength]
  protected val sizeFinder: SizeFinder[BagLocation]
  val tags: Option[Tags[BagLocation]]
  implicit val locator: Locatable[BagLocation, BagPrefix, URI]

  def check(
    expectedFileFixity: ExpectedFileFixity
  ): FileFixityResult[BagLocation] = {
    debug(s"Attempting to verify: $expectedFileFixity")

    val algorithm = expectedFileFixity.checksum.algorithm

    // The verifier writes a tag to the storage location after it's verified.
    //
    // If it tries to verify a location and finds the correct tag, it skips
    // re-reading the entire file.
    //
    // This means re-verifying the same content (e.g. a new version of a bag that
    // refers back to files in an older version):
    //
    //    1.  Is faster than reading the entire object again
    //    2.  Works even if the objects have been cycled to Glacier/cold storage
    //        (e.g. if they're very large and infrequently accessed)

    val fixityResult = for {
      location <- parseLocation(expectedFileFixity)
      _ = debug(
        s"Parsed location for ${expectedFileFixity.uri} as $location"
      )

      existingTags <- getExistingTags(expectedFileFixity, location)
      _ = debug(s"Got existing tags for $location: $existingTags")

      size <- verifySize(expectedFileFixity, location)
      _ = debug(s"Checked the size of $location is correct")

      result <- verifyChecksum(
        expectedFileFixity = expectedFileFixity,
        location = location,
        existingTags = existingTags,
        algorithm = algorithm,
        size = size
      )

      _ <- writeFixityTags(expectedFileFixity, location)
    } yield result

    debug(s"Fixity check result for ${expectedFileFixity.uri}: $fixityResult")

    fixityResult match {
      case Right(fixityCorrect) => fixityCorrect
      case Left(fixityError)    => fixityError
    }
  }

  private def parseLocation(
    expectedFileFixity: ExpectedFileFixity
  ): Either[FileFixityCouldNotRead[BagLocation], BagLocation] =
    expectedFileFixity.uri.locate match {
      case Right(location) => Right(location)
      case Left(locateError) =>
        Left(
          FileFixityCouldNotRead(
            expectedFileFixity = expectedFileFixity,
            e = LocationParsingError(expectedFileFixity, locateError.msg)
          )
        )
    }

  private def getExistingTags(
    expectedFileFixity: ExpectedFileFixity,
    location: BagLocation
  ): Either[FileFixityCouldNotRead[BagLocation], Map[String, String]] =
    handleReadErrors(
      tags
        .map(_.get(location).map(_.identifiedT))
        .getOrElse(Right(Map.empty[String, String])),
      expectedFileFixity = expectedFileFixity
    )

  private def openInputStream(
    expectedFileFixity: ExpectedFileFixity,
    location: BagLocation
  ): Either[FileFixityCouldNotRead[BagLocation], InputStreamWithLength] =
    handleReadErrors(
      streamReader.get(location),
      expectedFileFixity = expectedFileFixity
    ).map { _.identifiedT }

  private def verifySize(
    expectedFileFixity: ExpectedFileFixity,
    location: BagLocation
  ): Either[FileFixityError[BagLocation], Long] =
    for {
      actualLength <- handleReadErrors(
        sizeFinder.getSize(location),
        expectedFileFixity = expectedFileFixity
      )

      _ <- expectedFileFixity.length match {
        case Some(expectedLength) if expectedLength != actualLength =>
          Left(
            FileFixityMismatch(
              expectedFileFixity = expectedFileFixity,
              objectLocation = location,
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
    location: BagLocation,
    existingTags: Map[String, String],
    algorithm: HashingAlgorithm,
    size: Long
  ): Either[FileFixityError[BagLocation], FileFixityCorrect[BagLocation]] =
    existingTags.get(fixityTagName(expectedFileFixity)) match {
      case Some(cachedFixityValue)
          if cachedFixityValue == fixityTagValue(expectedFileFixity) =>
        Right(
          FileFixityCorrect(
            expectedFileFixity = expectedFileFixity,
            objectLocation = location,
            size = size
          )
        )

      case Some(_) =>
        Left(
          FileFixityMismatch(
            expectedFileFixity = expectedFileFixity,
            objectLocation = location,
            e = new Throwable(
              s"Cached verification tag doesn't match expected checksum for $location: $existingTags (${expectedFileFixity.checksum})"
            )
          )
        )

      case None =>
        openInputStream(expectedFileFixity, location) match {
          case Left(err) => Left(err)
          case Right(inputStream) =>
            verifyChecksumFromInputStream(
              expectedFileFixity = expectedFileFixity,
              location = location,
              inputStream = inputStream,
              algorithm = algorithm,
              size = size
            )
        }
    }

  private def verifyChecksumFromInputStream(
    expectedFileFixity: ExpectedFileFixity,
    location: BagLocation,
    inputStream: InputStreamWithLength,
    algorithm: HashingAlgorithm,
    size: Long
  ): Either[FileFixityError[BagLocation], FileFixityCorrect[BagLocation]] = {
    // This assertion should never fire in practice -- if it does, it means
    // an object is changing under our feet.  If that's the case, there's something
    // badly wrong with the storage service.
    //
    // There's nothing sensible we can do to recover, so throw immediately and wait
    // for a human to come and inspect the issue.
    assert(
      size == inputStream.length,
      message =
        s"The size of $location has changed!  Before: $size, after: ${inputStream.length}"
    )

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

  private def writeFixityTags(
    expectedFileFixity: ExpectedFileFixity,
    location: BagLocation
  ): Either[FileFixityCouldNotWriteTag[BagLocation], Unit] =
    tags
      .map { t =>
        t.update(location) { existingTags =>
          val tagName = fixityTagName(expectedFileFixity)
          val tagValue = fixityTagValue(expectedFileFixity)

          val fixityTags = Map(tagName -> tagValue)

          // We've already checked the tags on this location once, so we shouldn't
          // see conflicting values here.  Check we're not about to blat some existing
          // tags just in case.  If we do see conflicting tags here, there's something
          // badly wrong with the storage service.
          //
          // Note: this is a fairly weak guarantee, because tags aren't locked during
          // an update operation.
          assert(
            existingTags.getOrElse(tagName, tagValue) == tagValue,
            s"Trying to write $fixityTags to $location; existing tags conflict: $existingTags"
          )

          Right(existingTags ++ fixityTags)
        } match {
          case Right(_) => Right(())
          case Left(writeError) =>
            Left(
              FileFixityCouldNotWriteTag(
                expectedFileFixity = expectedFileFixity,
                objectLocation = location,
                e = writeError.e
              )
            )
        }
      }
      .getOrElse(Right(()))

  // e.g. Content-MD5, Content-SHA256
  private def fixityTagName(expectedFileFixity: ExpectedFileFixity): String =
    s"Content-${expectedFileFixity.checksum.algorithm.pathRepr.toUpperCase}"

  private def fixityTagValue(expectedFileFixity: ExpectedFileFixity): String =
    expectedFileFixity.checksum.value.toString

  private def handleReadErrors[T](
    t: Either[ReadError, T],
    expectedFileFixity: ExpectedFileFixity
  ): Either[FileFixityCouldNotRead[BagLocation], T] =
    t match {
      case Right(value) => Right(value)

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
}
