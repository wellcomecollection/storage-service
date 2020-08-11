package uk.ac.wellcome.platform.archive.bagverifier.fixity
import java.net.URI

import uk.ac.wellcome.platform.archive.common.bagit.models.BagPath
import uk.ac.wellcome.platform.archive.common.verify.Checksum

sealed trait ExpectedFileFixity{
  val uri: URI
  val path: BagPath
  val checksum: Checksum
  val length: Option[Long]
}

case class FetchFileFixity(
  uri: URI,
  path: BagPath,
  checksum: Checksum,
  length: Option[Long]
) extends ExpectedFileFixity

case class DataDirectoryFileFixity(
  uri: URI,
  path: BagPath,
  checksum: Checksum
) extends ExpectedFileFixity {
  val length: Option[Long] = None
}
