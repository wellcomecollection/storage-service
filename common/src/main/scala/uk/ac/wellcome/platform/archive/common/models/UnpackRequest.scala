package uk.ac.wellcome.platform.archive.common.models

import java.util.UUID

import uk.ac.wellcome.storage.ObjectLocation

case class UnpackRequest(
  requestId: UUID,
  sourceLocation: ObjectLocation,
  storageSpace: StorageSpace
)
