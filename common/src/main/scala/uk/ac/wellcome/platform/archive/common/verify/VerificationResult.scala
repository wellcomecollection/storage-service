package uk.ac.wellcome.platform.archive.common.verify

sealed trait VerificationResult

case class VerificationSuccess(locations: List[VerifiedSuccess])
    extends VerificationResult

case class VerificationFailure(failure: List[VerifiedFailure],
                               success: List[VerifiedSuccess])
    extends Throwable("Verification failure!")
    with VerificationResult

case class VerificationIncomplete(msg: String)
    extends Throwable(msg)
    with VerificationResult
