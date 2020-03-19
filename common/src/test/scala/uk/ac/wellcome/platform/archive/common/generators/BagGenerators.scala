package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.bagit.models.{
  Bag,
  BagFetch,
  BagFetchMetadata,
  BagPath,
  PayloadManifest,
  TagManifest
}
import uk.ac.wellcome.platform.archive.common.verify._

trait BagGenerators extends BagInfoGenerators {
  def createBagWith(
    manifestEntries: Map[BagPath, ChecksumValue] = Map.empty,
    manifestChecksumAlgorithm: HashingAlgorithm = SHA256,
    tagManifestEntries: Map[BagPath, ChecksumValue] = Map.empty,
    tagManifestChecksumAlgorithm: HashingAlgorithm = SHA256,
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

  def createBag: Bag = createBagWith()
}
