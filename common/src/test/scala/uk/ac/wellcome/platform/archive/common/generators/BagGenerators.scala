package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.bagit.models.{
  Bag,
  BagFetch,
  BagFetchMetadata,
  BagManifest,
  BagPath
}
import uk.ac.wellcome.platform.archive.common.verify.{
  Checksum,
  HashingAlgorithm,
  SHA256
}

trait BagGenerators extends BagInfoGenerators {
  def createBagWith(
    manifestEntries: Map[BagPath, Checksum] = Map.empty,
    manifestChecksumAlgorithm: HashingAlgorithm = SHA256,
    tagManifestEntries: Map[BagPath, Checksum] = Map.empty,
    tagManifestChecksumAlgorithm: HashingAlgorithm = SHA256,
    fetchEntries: Map[BagPath, BagFetchMetadata] = Map.empty
  ): Bag =
    Bag(
      info = createBagInfo,
      manifest = BagManifest(
        checksumAlgorithm = manifestChecksumAlgorithm,
        entries = manifestEntries
      ),
      tagManifest = BagManifest(
        checksumAlgorithm = tagManifestChecksumAlgorithm,
        entries = tagManifestEntries
      ),
      fetch = if (fetchEntries.isEmpty) None else Some(BagFetch(fetchEntries))
    )

  def createBag: Bag = createBagWith()
}
