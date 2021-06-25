package weco.storage_service.bagit.services.memory

import weco.fixtures.TestWith
import weco.storage_service.bagit.services.{
  BagReader,
  BagReaderTestCases
}
import weco.storage_service.fixtures.memory.MemoryBagBuilder
import weco.storage.providers.memory.{
  MemoryLocation,
  MemoryLocationPrefix
}
import weco.storage.store.TypedStore
import weco.storage.store.memory.{MemoryStreamStore, MemoryTypedStore}

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
    testWith(randomAlphanumeric())

  override def deleteFile(root: MemoryLocationPrefix, path: String)(
    implicit context: MemoryStreamStore[MemoryLocation]
  ): Unit =
    context.memoryStore.entries = context.memoryStore.entries.filter {
      case (location, _) => location != root.asLocation(path)
    }
}
