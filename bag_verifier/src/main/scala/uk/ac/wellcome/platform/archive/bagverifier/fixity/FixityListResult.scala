package uk.ac.wellcome.platform.archive.bagverifier.fixity

sealed trait FixityListResult

sealed trait FixityListCheckingResult extends FixityListResult

case class FixityListAllCorrect(locations: List[FixityCorrect])
    extends FixityListCheckingResult

case class FixityListWithErrors(
  errors: List[FixityError],
  correct: List[FixityCorrect]
) extends Throwable("Some files don't have the expected fixity (size/checksum)!")
    with FixityListCheckingResult

case class CouldNotCreateExpectedFixityList(msg: String)
    extends Throwable(msg)
    with FixityListResult
