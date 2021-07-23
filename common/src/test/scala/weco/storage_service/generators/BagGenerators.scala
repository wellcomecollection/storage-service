package weco.storage_service.generators

import weco.storage_service.bagit.models.{
  Bag,
  BagFetch,
  BagFetchMetadata,
  BagPath,
  NewPayloadManifest,
  NewTagManifest,
  PayloadManifest,
  TagManifest
}
import weco.storage_service.checksum._

trait BagGenerators extends BagInfoGenerators {
  def createNewBagWith(
    payloadManifestEntries: Map[BagPath, MultiManifestChecksum] = Map(),
    tagManifestEntries: Map[BagPath, MultiManifestChecksum] = Map(),
    fetchEntries: Map[BagPath, BagFetchMetadata] = Map()
  ): Bag =
    Bag(
      info = createBagInfo,
      newManifest = NewPayloadManifest(
        algorithms = payloadManifestEntries.values.headOption
          .map(_.definedAlgorithms)
          .getOrElse(ChecksumAlgorithms.algorithms),
        entries = payloadManifestEntries
      ),
      newTagManifest = NewTagManifest(
        algorithms = tagManifestEntries.values.headOption
          .map(_.definedAlgorithms)
          .getOrElse(ChecksumAlgorithms.algorithms),
        entries = tagManifestEntries
      ),
      fetch = if (fetchEntries.isEmpty) None else Some(BagFetch(fetchEntries))
    )

  def createBagWith(
    manifestEntries: Map[BagPath, ChecksumValue] = Map.empty,
    manifestChecksumAlgorithm: ChecksumAlgorithm = SHA256,
    tagManifestEntries: Map[BagPath, ChecksumValue] = Map.empty,
    tagManifestChecksumAlgorithm: ChecksumAlgorithm = SHA256,
    fetchEntries: Map[BagPath, BagFetchMetadata] = Map.empty
  ): Bag =
    Bag(
      info = createBagInfo,
      manifest = PayloadManifest(
        checksumAlgorithm = manifestChecksumAlgorithm,
        entries = manifestEntries
      ),
      tagManifest = TagManifest(
        checksumAlgorithm = tagManifestChecksumAlgorithm,
        entries = tagManifestEntries
      ),
      fetch = if (fetchEntries.isEmpty) None else Some(BagFetch(fetchEntries))
    )

  def createBag: Bag = createNewBagWith()
}
