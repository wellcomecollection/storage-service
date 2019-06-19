package uk.ac.wellcome.platform.archive.bagunpacker.models

import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation

case class BagUnpackerPayload(
  ingestId: IngestID,
  sourceLocation: ObjectLocation,
  storageSpace: StorageSpace
)
