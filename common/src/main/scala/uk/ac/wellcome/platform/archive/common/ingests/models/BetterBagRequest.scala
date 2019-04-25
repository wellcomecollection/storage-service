package uk.ac.wellcome.platform.archive.common.ingests.models

import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.BagLocation
import uk.ac.wellcome.storage.ObjectLocation

case class BetterBagRequest(
  ingestId: IngestID,
  bagLocation: BagLocation,
  bagRoot: ObjectLocation
)
