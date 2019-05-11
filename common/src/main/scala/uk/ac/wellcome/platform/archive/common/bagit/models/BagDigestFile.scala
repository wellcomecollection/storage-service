package uk.ac.wellcome.platform.archive.common.bagit.models

import uk.ac.wellcome.platform.archive.common.verify.ChecksumValue

case class BagDigestFile(checksum: ChecksumValue, path: BagItemPath)

