package uk.ac.wellcome.platform.archive.common.verify

import uk.ac.wellcome.storage.ObjectLocation

sealed trait VerifiedLocation

case class VerifiedSuccess(
  verifiableLocation: VerifiableLocation,
  objectLocation: ObjectLocation,
  size: Long
) extends VerifiedLocation

case class VerifiedFailure(
  objectLocation: Option[ObjectLocation],
  verificationError: VerificationError
) extends VerifiedLocation

case object VerifiedFailure {
  def apply(
    verificationError: VerificationError
  ): VerifiedFailure =
    VerifiedFailure(
      objectLocation = None,
      verificationError= verificationError
    )

  def apply(
    objectLocation: ObjectLocation,
    verificationError: VerificationError
  ): VerifiedFailure =
    VerifiedFailure(
      objectLocation = Some(objectLocation),
      verificationError = verificationError
    )
}
