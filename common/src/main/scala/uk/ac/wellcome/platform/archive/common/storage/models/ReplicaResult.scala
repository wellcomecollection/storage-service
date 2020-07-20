package uk.ac.wellcome.platform.archive.common.storage.models

import java.time.Instant

import uk.ac.wellcome.storage.S3ObjectLocationPrefix

case class ReplicaResult(
  originalLocation: S3ObjectLocationPrefix,
  storageLocation: StorageLocation,
  timestamp: Instant
)
