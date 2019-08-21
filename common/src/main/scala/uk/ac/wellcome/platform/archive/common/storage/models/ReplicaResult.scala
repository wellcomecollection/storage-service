package uk.ac.wellcome.platform.archive.common.storage.models

import java.time.Instant

case class ReplicaResult(
  storageLocation: StorageLocation,
  timestamp: Instant
)
