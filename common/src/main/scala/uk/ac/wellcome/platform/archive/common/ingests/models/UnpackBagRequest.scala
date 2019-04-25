package uk.ac.wellcome.platform.archive.common.ingests.models

import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation

case class UnpackBagRequest(
  ingestId: IngestID,
  sourceLocation: ObjectLocation,
  storageSpace: StorageSpace
)
