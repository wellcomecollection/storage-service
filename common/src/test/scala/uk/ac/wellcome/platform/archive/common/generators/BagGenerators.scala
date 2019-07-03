package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.bagit.models.{
  Bag,
  BagFetch,
  BagFetchEntry,
  BagFile,
  BagManifest
}
import uk.ac.wellcome.platform.archive.common.verify.SHA256

trait BagGenerators extends BagInfoGenerators {
  def createBagWith(
    manifestFiles: Seq[BagFile] = List.empty,
    tagManifestFiles: Seq[BagFile] = List.empty,
    fetchEntries: Seq[BagFetchEntry] = List.empty
  ): Bag =
    Bag(
      info = createBagInfo,
      manifest = BagManifest(
        checksumAlgorithm = SHA256,
        files = manifestFiles
      ),
      tagManifest = BagManifest(
        checksumAlgorithm = SHA256,
        files = tagManifestFiles
      ),
      fetch = if (fetchEntries.isEmpty) None else Some(BagFetch(fetchEntries))
    )

  def createBag: Bag = createBagWith()
}
