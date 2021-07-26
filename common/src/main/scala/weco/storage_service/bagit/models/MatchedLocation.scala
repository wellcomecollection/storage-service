package weco.storage_service.bagit.models

import weco.storage_service.checksum.{
  Checksum,
  ChecksumAlgorithm,
  MultiManifestChecksum
}

case class MatchedLocation(
  bagPath: BagPath,
  multiChecksum: MultiManifestChecksum,
  algorithm: ChecksumAlgorithm,
  fetchMetadata: Option[BagFetchMetadata]
) {

  // This is for backwards-compatibility purposes while we migrate to adding multi-checksum
  // support to the storage service, but we'll remove it later.
  def checksum: Checksum =
    Checksum(
      algorithm = algorithm,
      value = multiChecksum.getValue(algorithm).get
    )
}
