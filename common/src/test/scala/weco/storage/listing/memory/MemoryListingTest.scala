package weco.storage.listing.memory

import org.scalatest.Assertion
import weco.fixtures.RandomGenerators
import weco.storage.listing.ListingTestCases
import weco.storage.store.memory.MemoryStore

class MemoryListingTest
    extends ListingTestCases[
      String,
      String,
      String,
      MemoryListing[String, String, Array[Byte]],
      MemoryStore[String, Array[Byte]]]
    with MemoryListingFixtures[Array[Byte]]
    with RandomGenerators {
  def createT: Array[Byte] = randomBytes()

  override def createIdent(
    implicit context: MemoryStore[String, Array[Byte]]): String =
    randomAlphanumeric()

  override def extendIdent(id: String, extension: String): String =
    id + extension

  override def createPrefix: String = randomAlphanumeric()

  override def createPrefixMatching(id: String): String = id

  override def assertResultCorrect(result: Iterable[String],
                                   entries: Seq[String])(implicit context: MemoryStore[String, Array[Byte]]): Assertion =
    result.toSeq should contain theSameElementsAs entries
}
