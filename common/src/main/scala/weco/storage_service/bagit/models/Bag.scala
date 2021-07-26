package weco.storage_service.bagit.models

import weco.storage_service.checksum.{MultiManifestChecksum, SHA256}

case class Bag(
  info: BagInfo,
  newManifest: NewPayloadManifest,
  newTagManifest: NewTagManifest,
  fetch: Option[BagFetch]
) {
  def manifest: PayloadManifest =
    PayloadManifest(
      checksumAlgorithm = SHA256,
      entries = newManifest.entries.map {
        case (path, multiChecksum) =>
          path -> multiChecksum.sha256.get
      }
    )

  def tagManifest: TagManifest =
    TagManifest(
      checksumAlgorithm = SHA256,
      entries = newTagManifest.entries.map {
        case (path, multiChecksum) =>
          path -> multiChecksum.sha256.get
      }
    )

  // Quoting RFC 8493 § 2.2.1 (https://datatracker.ietf.org/doc/html/rfc8493#section-2.2.1):
  //
  //      Tag manifests SHOULD use the same algorithms as the payload manifests
  //      that are present in the bag.
  //
  // This should already have been checked by the BagReader before creating
  // an instance of this class, so we can display useful errors to users.
  // This assertion is meant to prevent against programmer error in the
  // storage service code, not malformed bags uploaded by users.
  //
  require(
    newManifest.algorithms == newTagManifest.algorithms,
    "Payload and tag manifests use different algorithms!"
  )
}

// These methods/assertions are for backwards compatibility only, and will
// be removed at the end of the work to support multiple checksums.
case object Bag {
  def apply(
    info: BagInfo,
    manifest: PayloadManifest,
    tagManifest: TagManifest,
    fetch: Option[BagFetch]
  ): Bag = {
    require(manifest.checksumAlgorithm == SHA256)
    require(tagManifest.checksumAlgorithm == SHA256)

    Bag(
      info = info,
      newManifest = NewPayloadManifest(
        algorithms = Set(SHA256),
        entries = manifest.entries
          .map {
            case (path, checksum) =>
              path -> MultiManifestChecksum(
                md5 = None,
                sha1 = None,
                sha256 = Some(checksum),
                sha512 = None
              )
          }
      ),
      newTagManifest = NewTagManifest(
        algorithms = Set(SHA256),
        entries = tagManifest.entries
          .map {
            case (path, checksum) =>
              path -> MultiManifestChecksum(
                md5 = None,
                sha1 = None,
                sha256 = Some(checksum),
                sha512 = None
              )
          }
      ),
      fetch = fetch
    )
  }
}
