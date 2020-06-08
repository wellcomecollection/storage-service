package uk.ac.wellcome.platform.archive.bagverifier.fixity
import java.net.URI

import uk.ac.wellcome.platform.archive.common.bagit.models.BagPath
import uk.ac.wellcome.platform.archive.common.verify.Checksum

case class ExpectedFileFixity(
  uri: URI,
  path: BagPath,
  checksum: Checksum,
  length: Option[Long]
)
