package uk.ac.wellcome.platform.archive.bagverifier.verify.steps

import uk.ac.wellcome.platform.archive.bagverifier.fixity.{FixityChecker, FixityListChecker, FixityListResult}
import uk.ac.wellcome.platform.archive.bagverifier.fixity.bag.BagExpectedFixity
import uk.ac.wellcome.platform.archive.bagverifier.models.BagVerifierError
import uk.ac.wellcome.platform.archive.bagverifier.storage.Resolvable
import uk.ac.wellcome.platform.archive.common.bagit.models.Bag
import uk.ac.wellcome.storage.{Location, ObjectLocation, ObjectLocationPrefix}

import scala.util.{Failure, Success, Try}

trait VerifyChecksumAndSize[BagLocation <: Location] {
  implicit val resolvable: Resolvable[ObjectLocation]
  implicit val fixityChecker: FixityChecker[BagLocation]

  def verifyChecksumAndSize(
    root: ObjectLocationPrefix,
    bag: Bag
  ): Either[BagVerifierError, FixityListResult] = {
    implicit val bagExpectedFixity: BagExpectedFixity =
      new BagExpectedFixity(root)

    implicit val fixityListChecker: FixityListChecker[BagLocation, Bag] =
      new FixityListChecker()

    Try { fixityListChecker.check(bag) } match {
      case Failure(err: Throwable) => Left(BagVerifierError(err))
      case Success(result)         => Right(result)
    }
  }
}
