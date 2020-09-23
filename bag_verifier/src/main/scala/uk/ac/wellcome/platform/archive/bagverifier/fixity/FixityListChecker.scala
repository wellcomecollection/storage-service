package uk.ac.wellcome.platform.archive.bagverifier.fixity

import grizzled.slf4j.Logging
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import uk.ac.wellcome.storage.{Location, Prefix}

import scala.util.Random

/** Given some Container of files, get the expected fixity information for every
  * file in the container, then verify the fixity on each of them.
  *
  */
class FixityListChecker[BagLocation <: Location, BagPrefix <: Prefix[
  BagLocation
], Container](
  implicit
  dataDirectoryFixityChecker: FixityChecker[BagLocation, BagPrefix],
  val fetchEntriesFixityChecker: FixityChecker[
    S3ObjectLocation,
    S3ObjectLocationPrefix
  ]
) extends Logging {

  def check(container: Container)(
    implicit expectedFixityCreator: ExpectedFixity[Container]
  ): FixityListResult[BagLocation] = {
    debug(s"Checking the fixity info for $container")
    expectedFixityCreator.create(container) match {
      case Left(err) => CouldNotCreateExpectedFixityList(err.msg)

      // The slow part of running the fixity checker is reading the entire
      // file to get the SHA-256 checksum, so all our fixity checkers treat
      // objects/blobs as immutable, and cache the result using a Tags[_].
      //
      // Shuffling the files means that if two bag verifiers are running in
      // parallel, they'll work through the files in a different order -- both
      // recording their results with Tags[_] as they go.
      //
      // In most cases this won't make any difference to the result; for really
      // large bags it gives us a way to speed up verification.
      case Right(expectedFileFixities: Seq[ExpectedFileFixity]) =>
        Random
          .shuffle(expectedFileFixities)
          .map {
            case f: FetchFileFixity => fetchEntriesFixityChecker.check(f)
            case d: DataDirectoryFileFixity =>
              dataDirectoryFixityChecker.check(d)
          }
          .foldLeft[FixityListCheckingResult[BagLocation]](
            FixityListAllCorrect(Nil)
          ) {

            case (
                existingCorrect: FixityListAllCorrect[BagLocation],
                newCorrect: FileFixityCorrect[BagLocation]
                ) =>
              FixityListAllCorrect(newCorrect :: existingCorrect.locations)

            case (
                correct: FixityListAllCorrect[BagLocation],
                err: FileFixityError[BagLocation]
                ) =>
              FixityListWithErrors(
                errors = List(err),
                correct = correct.locations
              )

            case (
                existingErrors: FixityListWithErrors[BagLocation],
                c: FileFixityCorrect[BagLocation]
                ) =>
              FixityListWithErrors(
                existingErrors.errors,
                c :: existingErrors.correct
              )

            case (
                existingErrors: FixityListWithErrors[BagLocation],
                err: FileFixityError[BagLocation]
                ) =>
              FixityListWithErrors(
                errors = err :: existingErrors.errors,
                correct = existingErrors.correct
              )
          }
    }
  }
}
