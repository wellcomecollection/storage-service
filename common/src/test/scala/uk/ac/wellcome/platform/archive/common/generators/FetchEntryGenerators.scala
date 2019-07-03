package uk.ac.wellcome.platform.archive.common.generators

import java.net.URI

import uk.ac.wellcome.platform.archive.common.bagit.models.{BagFetchEntry, BagPath}
import uk.ac.wellcome.storage.generators.RandomThings

trait FetchEntryGenerators extends RandomThings {
  def createFetchEntryWith(
    uri: String = s"http://example.org/$randomAlphanumeric",
    path: BagPath = BagPath(randomAlphanumeric)
  ): BagFetchEntry =
    BagFetchEntry(
      uri = new URI(uri),
      length = None,
      path = path
    )

  def createFetchEntry: BagFetchEntry =
    createFetchEntryWith()
}
