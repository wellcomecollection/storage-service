package uk.ac.wellcome.platform.archive.common.bagit.models

import java.net.URI

import uk.ac.wellcome.platform.archive.common.verify.Checksum

sealed trait BagLocation {
  val path: BagPath
}

case class BagFile(
  checksum: Checksum,
  path: BagPath
) extends BagLocation

case class BagFetchEntry(
  uri: URI,
  length: Option[Long],
  path: BagPath
) extends BagLocation
