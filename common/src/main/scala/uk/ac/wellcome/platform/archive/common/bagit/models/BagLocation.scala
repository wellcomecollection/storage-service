package uk.ac.wellcome.platform.archive.common.bagit.models

import uk.ac.wellcome.platform.archive.common.verify.Checksum

sealed trait BagLocation {
  val path: BagPath
}

case class BagFile(
  checksum: Checksum,
  path: BagPath
) extends BagLocation
