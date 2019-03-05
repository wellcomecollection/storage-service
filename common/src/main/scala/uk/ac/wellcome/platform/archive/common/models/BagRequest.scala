package uk.ac.wellcome.platform.archive.common.models

import java.util.UUID

import uk.ac.wellcome.platform.archive.common.models.bagit.BagLocation

case class BagRequest(
                       requestId: UUID,
                       bagLocation: BagLocation
)