package uk.ac.wellcome.platform.archive.bagunpacker.config

import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagLocation,
  BagPath
}
import uk.ac.wellcome.platform.archive.common.ingests.models.UnpackBagRequest
import uk.ac.wellcome.platform.archive.bagunpacker.config.models.UnpackerWorkerConfig

object BagLocationBuilder {
  def build(
    unpackBagRequest: UnpackBagRequest,
    unpackerWorkerConfig: UnpackerWorkerConfig
  ): BagLocation = {

    BagLocation(
      storageNamespace = unpackerWorkerConfig.dstNamespace,
      storagePrefix = unpackerWorkerConfig.maybeDstPrefix,
      storageSpace = unpackBagRequest.storageSpace,
      bagPath = BagPath(unpackBagRequest.requestId.toString)
    )
  }
}
