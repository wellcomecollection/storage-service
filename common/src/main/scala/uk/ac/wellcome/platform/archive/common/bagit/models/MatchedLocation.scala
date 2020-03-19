package uk.ac.wellcome.platform.archive.common.bagit.models

import uk.ac.wellcome.platform.archive.common.verify.Checksum

case class MatchedLocation(
  bagPath: BagPath,
  checksum: Checksum,
  fetchMetadata: Option[BagFetchMetadata]
)
