package weco.storage_service.generators

import java.net.URI

import weco.fixtures.RandomGenerators
import weco.storage_service.bagit.models.BagFetchMetadata

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
