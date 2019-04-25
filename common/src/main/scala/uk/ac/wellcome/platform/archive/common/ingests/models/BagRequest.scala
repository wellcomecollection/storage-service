package uk.ac.wellcome.platform.archive.common.ingests.models

import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.BagLocation

case class BagRequest(
  ingestId: IngestID,
  bagLocation: BagLocation
)
