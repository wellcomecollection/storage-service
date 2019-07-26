package uk.ac.wellcome.platform.archive.common.verify

import uk.ac.wellcome.storage.ObjectLocation

sealed trait VerifiedLocation

case class VerifiedSuccess(verifiableLocation: VerifiableLocation,
                           objectLocation: ObjectLocation,
                           size: Long)
    extends VerifiedLocation

case class VerifiedFailure(verifiableLocation: VerifiableLocation,
                           objectLocation: Option[ObjectLocation],
                           e: Throwable)
    extends VerifiedLocation

case object VerifiedFailure {
  def apply(verifiableLocation: VerifiableLocation,
            e: Throwable): VerifiedFailure =
    VerifiedFailure(
      verifiableLocation = verifiableLocation,
      objectLocation = None,
      e = e
    )

  def apply(
    verifiableLocation: VerifiableLocation,
    objectLocation: ObjectLocation,
    e: Throwable
  ): VerifiedFailure =
    VerifiedFailure(
      verifiableLocation = verifiableLocation,
      objectLocation = Some(objectLocation),
      e = e
    )
}
