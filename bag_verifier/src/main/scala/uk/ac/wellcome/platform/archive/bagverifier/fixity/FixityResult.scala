package uk.ac.wellcome.platform.archive.bagverifier.fixity

import uk.ac.wellcome.platform.archive.common.verify.VerifiableLocation
import uk.ac.wellcome.storage.ObjectLocation

sealed trait FixityResult {
  val verifiableLocation: VerifiableLocation
}

sealed trait FixityError extends FixityResult {
  val e: Throwable
}

case class FixityCorrect(
  verifiableLocation: VerifiableLocation,
  objectLocation: ObjectLocation,

  // We record the size of the files as we verify them, so we can verify
  // the Payload-Oxum in the bag metadata.
  size: Long
) extends FixityResult

case class FixityMismatch(
  verifiableLocation: VerifiableLocation,
  objectLocation: ObjectLocation,
  e: Throwable
) extends FixityError

case class FixityCouldNotRead(
  verifiableLocation: VerifiableLocation,
  e: Throwable
) extends FixityError
