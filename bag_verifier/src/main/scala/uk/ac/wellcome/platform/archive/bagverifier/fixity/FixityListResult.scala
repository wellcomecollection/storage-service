package uk.ac.wellcome.platform.archive.bagverifier.fixity

sealed trait FixityListResult

sealed trait FixityListCheckingResult extends FixityListResult

case class FixityListAllCorrect[BagLocation](locations: List[FileFixityCorrect[BagLocation]])
    extends FixityListCheckingResult

case class FixityListWithErrors[BagLocation](
  errors: List[FileFixityError[BagLocation]],
  correct: List[FileFixityCorrect[BagLocation]]
) extends Throwable(
      "Some files don't have the expected fixity (size/checksum)!"
    )
    with FixityListCheckingResult

case class CouldNotCreateExpectedFixityList(msg: String)
    extends Throwable(msg)
    with FixityListResult
