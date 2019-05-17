package uk.ac.wellcome.platform.archive.common.bagit.models

import uk.ac.wellcome.platform.archive.common.verify.ChecksumValue

case class BagFile(checksum: ChecksumValue, path: BagPath)
