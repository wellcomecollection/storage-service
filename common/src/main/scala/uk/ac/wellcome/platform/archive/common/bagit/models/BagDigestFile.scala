package uk.ac.wellcome.platform.archive.common.bagit.models

import uk.ac.wellcome.platform.archive.common.storage.Located

case class BagDigestFile(checksum: String, path: BagItemPath) extends Located

