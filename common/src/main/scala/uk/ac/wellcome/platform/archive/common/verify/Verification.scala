package uk.ac.wellcome.platform.archive.common.verify

import grizzled.slf4j.Logging

object Verification extends Logging {
  implicit class Verify[Container](container: Container)(
    implicit verifiable: Verifiable[Container],
    verifier: Verifier[_]
  ) {
    def verify: VerificationResult = {
      debug(s"Verification: Attempting to verify $container")
      verifiable.create(container) match {
        case Left(e) => VerificationIncomplete(e.msg)
        case Right(verifiableLocations) =>
          verifiableLocations
            .map(verifier.verify)
            .foldLeft[VerificationResult](VerificationSuccess(Nil)) {

            case (VerificationSuccess(sl), s @ VerifiedSuccess(_, _, _)) =>
              VerificationSuccess(s :: sl)

            case (VerificationSuccess(sl), f @ VerifiedFailure(_, _, _)) =>
              VerificationFailure(List(f), sl)

            case (
              VerificationFailure(fl, sl),
              s @ VerifiedSuccess(_, _, _)
              ) =>
              VerificationFailure(fl, s :: sl)

            case (
              VerificationFailure(fl, sl),
              f @ VerifiedFailure(_, _, _)
              ) =>
              VerificationFailure(f :: fl, sl)

            case (i @ VerificationIncomplete(_), _) => i
          }
      }
    }
  }
}
