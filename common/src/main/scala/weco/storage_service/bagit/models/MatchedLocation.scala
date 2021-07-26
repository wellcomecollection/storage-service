package weco.storage_service.bagit.models

import weco.storage_service.checksum.MultiManifestChecksum

case class MatchedLocation(
  bagPath: BagPath,
  multiChecksum: MultiManifestChecksum,
  fetchMetadata: Option[BagFetchMetadata]
)
