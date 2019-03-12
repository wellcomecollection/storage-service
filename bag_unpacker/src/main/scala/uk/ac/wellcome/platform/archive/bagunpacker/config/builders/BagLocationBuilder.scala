package uk.ac.wellcome.platform.archive.bagunpacker.config.builders

import uk.ac.wellcome.platform.archive.bagunpacker.config.models.UnpackerConfig
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagLocation, BagPath}
import uk.ac.wellcome.platform.archive.common.ingests.models.UnpackBagRequest

object BagLocationBuilder {
  def build(
    unpackBagRequest: UnpackBagRequest,
    unpackerConfig: UnpackerConfig
  ) = {

    BagLocation(
      storageNamespace = unpackerConfig.dstNamespace,
      storagePrefix = unpackerConfig.maybeDstPrefix,
      storageSpace = unpackBagRequest.storageSpace,
      bagPath = BagPath(unpackBagRequest.requestId.toString)
    )
  }
}
