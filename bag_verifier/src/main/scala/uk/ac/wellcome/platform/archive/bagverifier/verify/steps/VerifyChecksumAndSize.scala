package uk.ac.wellcome.platform.archive.bagverifier.verify.steps

import uk.ac.wellcome.platform.archive.bagverifier.fixity.{FixityChecker, FixityListChecker, FixityListResult}
import uk.ac.wellcome.platform.archive.bagverifier.fixity.bag.BagExpectedFixity
import uk.ac.wellcome.platform.archive.common.bagit.models.Bag
import uk.ac.wellcome.platform.archive.common.storage.Resolvable
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

import scala.util.{Failure, Success, Try}

trait VerifyChecksumAndSize extends Step {
  implicit val resolvable: Resolvable[ObjectLocation]
  implicit val fixityChecker: FixityChecker

  def verifyChecksumAndSize(
    root: ObjectLocationPrefix,
    bag: Bag
  ): InternalResult[FixityListResult] = {
    implicit val bagExpectedFixity: BagExpectedFixity =
      new BagExpectedFixity(root.asLocation())

    implicit val fixityListChecker: FixityListChecker[Bag] =
      new FixityListChecker()

    Try { fixityListChecker.check(bag) } match {
      case Failure(err: Throwable) => Left(BagVerifierError(err))
      case Success(result)         => Right(result)
    }
  }
}
