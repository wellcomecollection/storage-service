package weco.storage_service.bag_verifier.verify.steps

import weco.storage_service.bag_verifier.fixity.bag.BagExpectedFixity
import weco.storage_service.bag_verifier.fixity.{
  FixityListChecker,
  FixityListResult
}
import weco.storage_service.bag_verifier.models.BagVerifierError
import weco.storage_service.bag_verifier.storage.Resolvable
import weco.storage_service.bagit.models.Bag
import weco.storage.{Location, Prefix}

import scala.util.{Failure, Success, Try}

trait VerifyChecksumAndSize[BagLocation <: Location,
                            BagPrefix <: Prefix[
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
