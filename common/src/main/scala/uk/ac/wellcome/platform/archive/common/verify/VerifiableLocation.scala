package uk.ac.wellcome.platform.archive.common.verify

import java.net.URI

case class VerifiableLocation(
  uri: URI,
  checksum: Checksum,
  length: Option[Long]
)
