package uk.ac.wellcome.platform.archive.bagunpacker.builders

import uk.ac.wellcome.platform.archive.bagunpacker.config.models.BagUnpackerWorkerConfig
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagLocation, BagPath}
import uk.ac.wellcome.platform.archive.common.ingests.models.UnpackBagRequest

object BagLocationBuilder {
  def build(
    unpackBagRequest: UnpackBagRequest,
    unpackerWorkerConfig: BagUnpackerWorkerConfig
  ): BagLocation = {

    BagLocation(
      storageNamespace = unpackerWorkerConfig.dstNamespace,
      storagePrefix = unpackerWorkerConfig.maybeDstPrefix,
      storageSpace = unpackBagRequest.storageSpace,
      bagPath = BagPath(unpackBagRequest.requestId.toString)
    )
  }
}
