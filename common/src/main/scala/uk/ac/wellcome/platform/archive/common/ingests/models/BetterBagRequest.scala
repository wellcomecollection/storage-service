package uk.ac.wellcome.platform.archive.common.ingests.models

import java.util.UUID

import uk.ac.wellcome.platform.archive.common.bagit.models.BagLocation
import uk.ac.wellcome.storage.ObjectLocation

case class BetterBagRequest(
  requestId: UUID,
  bagLocation: BagLocation,
  bagRoot: ObjectLocation
)
