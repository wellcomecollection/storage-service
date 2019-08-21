package uk.ac.wellcome.platform.archive.common.bagit.services.memory

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.bagit.services.{
  BagReader,
  BagReaderTestCases
}
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}
import uk.ac.wellcome.storage.store.TypedStore
import uk.ac.wellcome.storage.store.memory.{MemoryStreamStore, MemoryTypedStore}

class MemoryBagReaderTest
    extends BagReaderTestCases[MemoryStreamStore[ObjectLocation], String] {
  override def withContext[R](
    testWith: TestWith[MemoryStreamStore[ObjectLocation], R]
  ): R =
    testWith(MemoryStreamStore[ObjectLocation]())

  override def withTypedStore[R](
    testWith: TestWith[TypedStore[ObjectLocation, String], R]
  )(implicit context: MemoryStreamStore[ObjectLocation]): R =
    testWith(
      new MemoryTypedStore[ObjectLocation, String]() {
        override val streamStore: MemoryStreamStore[ObjectLocation] = context
      }
    )

  override def withBagReader[R](
    testWith: TestWith[BagReader[_], R]
  )(implicit context: MemoryStreamStore[ObjectLocation]): R =
    testWith(
      new MemoryBagReader()
    )

  override def withNamespace[R](testWith: TestWith[String, R]): R =
    testWith(randomAlphanumeric)

  override def deleteFile(root: ObjectLocationPrefix, path: String)(
    implicit context: MemoryStreamStore[ObjectLocation]
  ): Unit =
    context.memoryStore.entries = context.memoryStore.entries.filter {
      case (location, _) => location != root.asLocation(path)
    }
}
