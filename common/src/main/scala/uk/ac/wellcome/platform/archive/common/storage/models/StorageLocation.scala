package uk.ac.wellcome.platform.archive.common.storage.models

import uk.ac.wellcome.platform.archive.common.ingests.models.StorageProvider
import uk.ac.wellcome.storage.s3.S3ObjectLocationPrefix

sealed trait StorageLocation {
  val provider: StorageProvider
  val prefix: S3ObjectLocationPrefix
}

case class PrimaryStorageLocation(
  provider: StorageProvider,
  prefix: S3ObjectLocationPrefix
) extends StorageLocation

case class SecondaryStorageLocation(
  provider: StorageProvider,
  prefix: S3ObjectLocationPrefix
) extends StorageLocation
