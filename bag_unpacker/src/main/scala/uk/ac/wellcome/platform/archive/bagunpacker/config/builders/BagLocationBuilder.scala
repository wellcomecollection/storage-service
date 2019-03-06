package uk.ac.wellcome.platform.archive.bagunpacker.config.builders

import uk.ac.wellcome.platform.archive.bagunpacker.config.models.BagUnpackerConfig
import uk.ac.wellcome.platform.archive.common.models.UnpackBagRequest
import uk.ac.wellcome.platform.archive.common.models.bagit.{
  BagLocation,
  BagPath
}

object BagLocationBuilder {
  def build(
    unpackBagRequest: UnpackBagRequest,
    bagUnpackerConfig: BagUnpackerConfig
  ) = {

    BagLocation(
      storageNamespace = bagUnpackerConfig.dstNamespace,
      storagePrefix = bagUnpackerConfig.maybeDstPrefix,
      storageSpace = unpackBagRequest.storageSpace,
      bagPath = BagPath(unpackBagRequest.requestId.toString)
    )
  }
}
