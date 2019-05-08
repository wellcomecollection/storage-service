package uk.ac.wellcome.platform.archive.bagverifier.models

import uk.ac.wellcome.storage.ObjectLocation

case class VerificationRequest(
  objectLocation: ObjectLocation,
  checksum: Checksum
)
