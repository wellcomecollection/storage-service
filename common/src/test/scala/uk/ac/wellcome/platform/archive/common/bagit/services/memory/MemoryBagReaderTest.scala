package uk.ac.wellcome.platform.archive.common.bagit.services.memory

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.bagit.services.{
  BagReader,
  BagReaderTestCases
}
import uk.ac.wellcome.platform.archive.common.fixtures.memory.MemoryBagBuilder
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.store.TypedStore
import uk.ac.wellcome.storage.store.memory.{
  MemoryStreamStore,
  MemoryTypedStore
}

class MemoryBagReaderTest
    extends BagReaderTestCases[MemoryStreamStore[MemoryLocation], String, MemoryLocation, MemoryLocationPrefix]
    with MemoryBagBuilder {

  override def withContext[R](
    testWith: TestWith[MemoryStreamStore[MemoryLocation], R]
  ): R =
    testWith(MemoryStreamStore[MemoryLocation]())

  override def withTypedStore[R](
    testWith: TestWith[TypedStore[MemoryLocation, String], R]
  )(implicit context: MemoryStreamStore[MemoryLocation]): R =
    testWith(
      new MemoryTypedStore[MemoryLocation, String]() {
        override val streamStore: MemoryStreamStore[MemoryLocation] = context
      }
    )

  override def withBagReader[R](
    testWith: TestWith[BagReader[MemoryLocation, MemoryLocationPrefix], R]
  )(implicit context: MemoryStreamStore[MemoryLocation]): R =
    testWith(new MemoryBagReader())

  override def withNamespace[R](testWith: TestWith[String, R]): R =
    testWith(randomAlphanumeric)

  override def deleteFile(root: MemoryLocationPrefix, path: String)(
    implicit context: MemoryStreamStore[MemoryLocation]
  ): Unit =
    context.memoryStore.entries = context.memoryStore.entries.filter {
      case (location, _) => location != root.asLocation(path)
    }
}
