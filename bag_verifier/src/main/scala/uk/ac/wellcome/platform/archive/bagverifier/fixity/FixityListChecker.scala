package uk.ac.wellcome.platform.archive.bagverifier.fixity

import grizzled.slf4j.Logging

/** Given some Container of files, get the expected fixity information for every
  * file in the container, then verify the fixity on each of them.
  *
  */
class FixityListChecker[Container](
  implicit
  verifiable: ExpectedFixity[Container],
  fixityChecker: FixityChecker[_]
) extends Logging {
  def check(container: Container): FixityListResult = {
    debug(s"Checking the fixity info for $container")
    verifiable.create(container) match {
      case Left(err) => CouldNotCreateExpectedFixityList(err.msg)
      case Right(verifiableLocations) =>
        verifiableLocations
          .map(fixityChecker.check)
          .foldLeft[FixityListCheckingResult](FixityListAllCorrect(Nil)) {

            case (
                FixityListAllCorrect(locations),
                correct: FileFixityCorrect
                ) =>
              FixityListAllCorrect(correct :: locations)

            case (FixityListAllCorrect(locations), err: FileFixityError) =>
              FixityListWithErrors(
                errors = List(err),
                correct = locations
              )

            case (
                FixityListWithErrors(errors, correct),
                c: FileFixityCorrect
                ) =>
              FixityListWithErrors(errors, c :: correct)

            case (
                FixityListWithErrors(errors, correct),
                err: FileFixityError
                ) =>
              FixityListWithErrors(
                errors = err :: errors,
                correct = correct
              )
          }
    }
  }
}
