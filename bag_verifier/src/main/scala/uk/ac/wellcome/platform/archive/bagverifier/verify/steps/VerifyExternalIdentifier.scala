package uk.ac.wellcome.platform.archive.bagverifier.verify.steps

import uk.ac.wellcome.platform.archive.common.bagit.models.{Bag, ExternalIdentifier}

trait VerifyExternalIdentifier extends Step {
  def verifyExternalIdentifier(
    bag: Bag,
    externalIdentifier: ExternalIdentifier
  ): InternalResult[Unit] =
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
