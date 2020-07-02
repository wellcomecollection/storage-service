package uk.ac.wellcome.platform.archive.bagverifier.fixity

import grizzled.slf4j.Logging
import uk.ac.wellcome.storage.Location

/** Given some Container of files, get the expected fixity information for every
  * file in the container, then verify the fixity on each of them.
  *
  */
class FixityListChecker[BagLocation <: Location, Container](
  implicit
  verifiable: ExpectedFixity[Container],
  fixityChecker: FixityChecker[BagLocation]
) extends Logging {
  def check(container: Container): FixityListResult[BagLocation] = {
    debug(s"Checking the fixity info for $container")
    verifiable.create(container) match {
      case Left(err) => CouldNotCreateExpectedFixityList(err.msg)
      case Right(verifiableLocations) =>
        verifiableLocations
          .map(fixityChecker.check)
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
