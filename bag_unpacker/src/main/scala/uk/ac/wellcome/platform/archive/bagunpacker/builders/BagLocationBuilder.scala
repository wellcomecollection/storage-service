package uk.ac.wellcome.platform.archive.bagunpacker.builders

import java.nio.file.Paths

import uk.ac.wellcome.platform.archive.bagunpacker.config.models.BagUnpackerWorkerConfig
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation

object BagLocationBuilder {
  def build(
    ingestId: IngestID,
    storageSpace: StorageSpace,
    unpackerWorkerConfig: BagUnpackerWorkerConfig
  ): ObjectLocation =
    ObjectLocation(
      namespace = unpackerWorkerConfig.dstNamespace,
      key = Paths
        .get(
          unpackerWorkerConfig.maybeDstPrefix.getOrElse(""),
          storageSpace.toString,
          ingestId.toString
        )
        .toString
    )
}
