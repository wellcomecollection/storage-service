package uk.ac.wellcome.platform.archive.common.ingests.models

import java.util.UUID

import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation

case class UnpackBagRequest(
  requestId: UUID,
  sourceLocation: ObjectLocation,
  storageSpace: StorageSpace
)
