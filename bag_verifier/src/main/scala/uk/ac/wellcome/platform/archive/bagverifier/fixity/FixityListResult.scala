package uk.ac.wellcome.platform.archive.bagverifier.fixity

sealed trait FixityListResult[BagLocation]

sealed trait FixityListCheckingResult[BagLocation]
    extends FixityListResult[BagLocation]

case class FixityListAllCorrect[BagLocation](
  locations: List[FileFixityCorrect[BagLocation]]
) extends FixityListCheckingResult[BagLocation]

case class FixityListWithErrors[BagLocation](
  errors: List[FileFixityError[BagLocation]],
  correct: List[FileFixityCorrect[BagLocation]]
) extends Throwable(
      "Some files don't have the expected fixity (size/checksum)!"
    )
    with FixityListCheckingResult[BagLocation]

case class CouldNotCreateExpectedFixityList[BagLocation](msg: String)
    extends Throwable(msg)
    with FixityListResult[BagLocation]
