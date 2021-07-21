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
      entries = newManifest.entries.map {
        case (path, multiChecksum) =>
          path -> multiChecksum.sha256.get
      }
    )
}

case object Bag {
  def apply(
    info: BagInfo,
    manifest: PayloadManifest,
    tagManifest: TagManifest,
    fetch: Option[BagFetch]): Bag =
    Bag(
      info = info,
      newManifest = NewPayloadManifest(
        entries = manifest.entries
          .map { case (path, checksum) =>
            path -> MultiManifestChecksum(sha256 = Some(checksum))
          }
      ),
      newTagManifest = NewTagManifest(
        entries = tagManifest.entries
          .map { case (path, checksum) =>
            path -> MultiManifestChecksum(sha256 = Some(checksum))
          }
      ),
      fetch = fetch
    )
}
