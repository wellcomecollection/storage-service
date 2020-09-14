package uk.ac.wellcome.platform.archive.bagverifier.verify.steps

import uk.ac.wellcome.platform.archive.bagverifier.fixity.bag.BagExpectedFixity
import uk.ac.wellcome.platform.archive.bagverifier.fixity.{
  FixityListChecker,
  FixityListResult
}
import uk.ac.wellcome.platform.archive.bagverifier.models.BagVerifierError
import uk.ac.wellcome.platform.archive.bagverifier.storage.Resolvable
import uk.ac.wellcome.platform.archive.common.bagit.models.Bag
import uk.ac.wellcome.storage.{Location, Prefix}

import scala.util.{Failure, Success, Try}

trait VerifyChecksumAndSize[BagLocation <: Location, BagPrefix <: Prefix[
  BagLocation
]] {
  implicit val resolvable: Resolvable[BagLocation]
  val fixityListChecker: FixityListChecker[BagLocation, BagPrefix, Bag]

  def verifyChecksumAndSize(
    root: BagPrefix,
    bag: Bag
  ): Either[BagVerifierError, FixityListResult[BagLocation]] = {
    implicit val bagExpectedFixity: BagExpectedFixity[BagLocation, BagPrefix] =
      new BagExpectedFixity[BagLocation, BagPrefix](root)

    Try { fixityListChecker.check(bag) } match {
      case Failure(err: Throwable) => Left(BagVerifierError(err))
      case Success(result)         => Right(result)
    }
  }
}
