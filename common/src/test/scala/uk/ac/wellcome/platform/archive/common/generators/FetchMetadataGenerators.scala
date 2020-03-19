package uk.ac.wellcome.platform.archive.common.generators

import java.net.URI

import uk.ac.wellcome.platform.archive.common.bagit.models.BagFetchMetadata
import uk.ac.wellcome.storage.generators.RandomThings

import scala.util.Random

trait FetchMetadataGenerators extends RandomThings {
  def createFetchMetadataWith(
    uri: String = s"http://example.org/$randomAlphanumeric",
    length: Option[Long] = createLength
  ): BagFetchMetadata =
    BagFetchMetadata(uri = new URI(uri), length = length)

  def createFetchMetadataWith(uri: String, length: Long): BagFetchMetadata =
    createFetchMetadataWith(uri = uri, length = Some(length))

  def createFetchMetadata: BagFetchMetadata =
    createFetchMetadataWith()

  private def createLength: Option[Long] =
    if (Random.nextBoolean()) {
      Some(Random.nextLong())
    } else {
      None
    }
}
