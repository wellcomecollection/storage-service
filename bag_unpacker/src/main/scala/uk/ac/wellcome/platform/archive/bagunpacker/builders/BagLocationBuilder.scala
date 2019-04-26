package uk.ac.wellcome.platform.archive.bagunpacker.builders

import java.nio.file.Paths

import uk.ac.wellcome.platform.archive.bagunpacker.config.models.BagUnpackerWorkerConfig
import uk.ac.wellcome.platform.archive.common.ingests.models.UnpackBagRequest
import uk.ac.wellcome.storage.ObjectLocation

object BagLocationBuilder {
  def build(
    unpackBagRequest: UnpackBagRequest,
    unpackerWorkerConfig: BagUnpackerWorkerConfig
  ): ObjectLocation =
    ObjectLocation(
      namespace = unpackerWorkerConfig.dstNamespace,
      key =
        Paths
          .get(
            unpackerWorkerConfig.maybeDstPrefix.getOrElse(""),
            unpackBagRequest.storageSpace.toString,
            unpackBagRequest.ingestId.toString
          )
          .toString
    )
}
