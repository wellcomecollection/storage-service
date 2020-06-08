package uk.ac.wellcome.platform.archive.bagverifier.fixity

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.verify._

/** Given some Container of files, get the expected fixity information for every
  * file in the container, then verify the fixity on each of them.
  *
  */
class FixityListChecker[Container](
  implicit
  verifiable: Verifiable[Container],
  fixityChecker: FixityChecker
) extends Logging {
  def verify(container: Container): VerificationResult = {
    debug(s"Checking the fixity info for $container")
    verifiable.create(container) match {
      case Left(e) => VerificationIncomplete(e.msg)
      case Right(verifiableLocations) =>
        verifiableLocations
          .map(fixityChecker.verify)
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
