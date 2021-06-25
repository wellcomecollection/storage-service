package weco.storage_service.bag_verifier.verify.steps

import weco.storage_service.bag_verifier.models.BagVerifierError
import weco.storage_service.bagit.models.{
  Bag,
  ExternalIdentifier
}

trait VerifyExternalIdentifier {
  def verifyExternalIdentifier(
    bag: Bag,
    externalIdentifier: ExternalIdentifier
  ): Either[BagVerifierError, Unit] =
    if (bag.info.externalIdentifier != externalIdentifier) {
      Left(
        BagVerifierError(
          "External identifier in bag-info.txt does not match request: " +
            s"${bag.info.externalIdentifier.underlying} is not ${externalIdentifier.underlying}"
        )
      )
    } else {
      Right(())
    }
}
