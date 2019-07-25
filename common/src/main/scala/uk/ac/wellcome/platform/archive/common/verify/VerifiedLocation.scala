package uk.ac.wellcome.platform.archive.common.verify

sealed trait VerifiedLocation

case class VerifiedSuccess(location: VerifiableLocation, size: Long)
    extends VerifiedLocation
case class VerifiedFailure(location: VerifiableLocation, e: Throwable)
    extends VerifiedLocation
