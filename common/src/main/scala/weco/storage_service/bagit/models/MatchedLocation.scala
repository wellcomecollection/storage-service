package weco.storage_service.bagit.models

import weco.storage_service.checksum.{Checksum, MultiManifestChecksum, SHA256}

case class MatchedLocation(
  bagPath: BagPath,
  multiChecksum: MultiManifestChecksum,
  fetchMetadata: Option[BagFetchMetadata]
) {
  require(multiChecksum.definedAlgorithms.contains(SHA256))

  def checksum: Checksum =
    Checksum(
      algorithm = SHA256,
      value = multiChecksum.getValue(SHA256).get
    )
}
