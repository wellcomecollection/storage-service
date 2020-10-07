package uk.ac.wellcome.platform.archive.common.generators

import java.net.URI

import uk.ac.wellcome.fixtures.RandomGenerators
import uk.ac.wellcome.platform.archive.common.bagit.models.BagFetchMetadata

import scala.util.Random

trait FetchMetadataGenerators extends RandomGenerators {
  def createFetchMetadataWith(
    uri: String = s"http://example.org/${randomAlphanumeric()}",
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
