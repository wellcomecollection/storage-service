package uk.ac.wellcome.platform.archive.common.generators

import java.net.URI

import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagFetchEntry,
  BagFetchMetadata,
  BagPath
}
import uk.ac.wellcome.storage.generators.RandomThings

import scala.util.Random

trait FetchEntryGenerators extends RandomThings {
  def createFetchMetadataWith(
    uri: String = s"http://example.org/$randomAlphanumeric",
    length: Option[Long] = createLength,
  ): BagFetchMetadata =
    BagFetchMetadata(uri = new URI(uri), length = length)

  def createFetchMetadataWith(uri: String, length: Long): BagFetchMetadata =
    createFetchMetadataWith(uri = uri, length = Some(length))

  def createFetchMetadata: BagFetchMetadata =
    createFetchMetadataWith()

  def createFetchEntryWith(
    uri: String = s"http://example.org/$randomAlphanumeric",
    length: Option[Long] = createLength,
    path: BagPath = BagPath(randomAlphanumeric)
  ): BagFetchEntry =
    BagFetchEntry(
      uri = new URI(uri),
      length = length,
      path = path
    )

  private def createLength: Option[Long] =
    if (Random.nextBoolean()) {
      Some(Random.nextLong())
    } else {
      None
    }

  def createFetchEntry: BagFetchEntry =
    createFetchEntryWith()
}
