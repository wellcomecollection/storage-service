package uk.ac.wellcome.platform.archive.common.verify

import java.net.URI

import uk.ac.wellcome.platform.archive.common.bagit.models.BagPath

case class VerifiableLocation(
  uri: URI,
  path: BagPath,
  checksum: Checksum,
  length: Option[Long]
)
