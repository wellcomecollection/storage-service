package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.bagit.models.{
  Bag,
  BagFetch,
  BagFetchEntry,
  BagFile,
  BagManifest
}
import uk.ac.wellcome.platform.archive.common.verify.{HashingAlgorithm, SHA256}

trait BagGenerators extends BagInfoGenerators {
  def createBagWith(
    manifestFiles: Seq[BagFile] = List.empty,
    manifestChecksumAlgorithm: HashingAlgorithm = SHA256,
    tagManifestFiles: Seq[BagFile] = List.empty,
    tagManifestChecksumAlgorithm: HashingAlgorithm = SHA256,
    fetchEntries: Seq[BagFetchEntry] = List.empty
  ): Bag =
    Bag(
      info = createBagInfo,
      manifest = BagManifest(
        checksumAlgorithm = manifestChecksumAlgorithm,
        files = manifestFiles
      ),
      tagManifest = BagManifest(
        checksumAlgorithm = tagManifestChecksumAlgorithm,
        files = tagManifestFiles
      ),
      fetch = if (fetchEntries.isEmpty) None else Some(BagFetch(fetchEntries))
    )

  def createBag: Bag = createBagWith()
}
