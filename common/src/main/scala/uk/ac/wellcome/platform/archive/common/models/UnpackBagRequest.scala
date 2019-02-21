package uk.ac.wellcome.platform.archive.common.models

import java.util.UUID

import uk.ac.wellcome.platform.archive.common.models.bagit.BagLocation
import uk.ac.wellcome.storage.ObjectLocation

case class UnpackBagRequest(
  requestId: UUID,
  packedBagLocation: ObjectLocation,
  bagDestination: BagLocation
)
