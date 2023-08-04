package weco.storage.services.memory

import java.util.UUID

import weco.fixtures.TestWith
import weco.storage.services.{SizeFinder, SizeFinderTestCases}
import weco.storage.store.memory.MemoryStore

class MemorySizeFinderTest
    extends SizeFinderTestCases[UUID, MemoryStore[UUID, Array[Byte]]] {
  type StoreImpl = MemoryStore[UUID, Array[Byte]]

  override def withContext[R](testWith: TestWith[StoreImpl, R]): R =
    testWith(
      new MemoryStore[UUID, Array[Byte]](initialEntries = Map.empty)
    )

  override def withSizeFinder[R](
    testWith: TestWith[SizeFinder[UUID], R]
  )(implicit underlyingStore: StoreImpl): R = {
    val sizeFinder = new MemorySizeFinder[UUID](
      memoryStore = underlyingStore
    )

    testWith(sizeFinder)
  }

  override def createIdent(implicit underlyingStore: StoreImpl): UUID =
    UUID.randomUUID()

  override def createObject(ident: UUID, contents: String)(
    implicit underlyingStore: StoreImpl
  ): Unit = {
    underlyingStore.put(ident)(contents.getBytes())
  }
}
