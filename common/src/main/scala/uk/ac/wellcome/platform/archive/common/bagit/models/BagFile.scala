package uk.ac.wellcome.platform.archive.common.bagit.models

import uk.ac.wellcome.platform.archive.common.verify.Checksum

case class BagFile(
  checksum: Checksum,
  path: BagPath
)
