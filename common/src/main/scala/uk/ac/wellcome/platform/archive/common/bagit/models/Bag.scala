package uk.ac.wellcome.platform.archive.common.bagit.models

import uk.ac.wellcome.platform.archive.common.storage.models.FileManifest

case class Bag(
                info: BagInfo,
                manifest: FileManifest,
                tagManifest: FileManifest
              )
