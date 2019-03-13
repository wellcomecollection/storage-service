package uk.ac.wellcome.platform.archive.common.ingests.models

import java.util.UUID

import uk.ac.wellcome.platform.archive.common.bagit.models.BagLocation

case class BagRequest(
  requestId: UUID,
  bagLocation: BagLocation
)
