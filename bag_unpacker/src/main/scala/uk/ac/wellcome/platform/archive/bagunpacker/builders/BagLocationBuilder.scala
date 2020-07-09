package uk.ac.wellcome.platform.archive.bagunpacker.builders

import java.nio.file.Paths

import uk.ac.wellcome.platform.archive.bagunpacker.config.models.BagUnpackerWorkerConfig
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.S3ObjectLocationPrefix

object BagLocationBuilder {
  def build(
    ingestId: IngestID,
    storageSpace: StorageSpace,
    unpackerWorkerConfig: BagUnpackerWorkerConfig
  ): S3ObjectLocationPrefix =
    S3ObjectLocationPrefix(
      bucket = unpackerWorkerConfig.dstNamespace,
      keyPrefix = Paths
        .get(
          storageSpace.toString,
          ingestId.toString
        )
        .toString
    )
}
