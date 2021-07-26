package weco.storage_service.generators

import weco.storage_service.bagit.models.{
  Bag,
  BagFetch,
  BagFetchMetadata,
  BagPath,
  PayloadManifest,
  TagManifest
}
import weco.storage_service.checksum._

trait BagGenerators extends BagInfoGenerators {
  def createBagWith(
    manifestEntries: Map[BagPath, MultiManifestChecksum] = Map.empty,
    manifestAlgorithms: Set[ChecksumAlgorithm] = Set(SHA256),
    tagManifestEntries: Map[BagPath, MultiManifestChecksum] = Map.empty,
    tagManifestAlgorithms: Set[ChecksumAlgorithm] = Set(SHA256),
    fetchEntries: Map[BagPath, BagFetchMetadata] = Map.empty
  ): Bag =
    Bag(
      info = createBagInfo,
      newManifest = PayloadManifest(
        algorithms = manifestAlgorithms,
        entries = manifestEntries
      ),
      newTagManifest = TagManifest(
        algorithms = tagManifestAlgorithms,
        entries = tagManifestEntries
      ),
      fetch = if (fetchEntries.isEmpty) None else Some(BagFetch(fetchEntries))
    )

  def createBag: Bag = createBagWith()
}
