package uk.ac.wellcome.platform.archive.common.verify

import uk.ac.wellcome.platform.archive.common.storage.LocateFailure
import uk.ac.wellcome.storage.ReadError

sealed trait VerificationError {
  val verifiableLocation: VerifiableLocation
}

case class VerificationChecksumError(
  verifiableLocation: VerifiableLocation,
  checksumFailure: FailedChecksum
) extends VerificationError

case class VerificationLocationError[T](
  verifiableLocation: VerifiableLocation,
  locateFailure: LocateFailure[T]
) extends VerificationError

case class VerificationReadError(
  verifiableLocation: VerifiableLocation,
  error: ReadError
) extends VerificationError

case class VerificationLengthsDoNotMatch(
  verifiableLocation: VerifiableLocation,
  expectedLength: Long,
  actualLength: Long
) extends VerificationError
