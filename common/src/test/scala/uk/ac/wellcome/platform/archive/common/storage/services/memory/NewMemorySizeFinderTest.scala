package uk.ac.wellcome.platform.archive.common.storage.services.memory

import java.util.UUID

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.storage.services.{NewSizeFinder, NewSizeFinderTestCases}
import uk.ac.wellcome.storage.store.memory.MemoryStore

class NewMemorySizeFinderTest extends NewSizeFinderTestCases[UUID, MemoryStore[UUID, Array[Byte]]] {
  type StoreImpl = MemoryStore[UUID, Array[Byte]]

  override def withContext[R](testWith: TestWith[StoreImpl, R]): R =
    testWith(
      new MemoryStore[UUID, Array[Byte]](initialEntries = Map.empty)
    )

  override def withSizeFinder[R](testWith: TestWith[NewSizeFinder[UUID], R])(
      implicit underlyingStore: StoreImpl): R = {
    val sizeFinder = new NewMemorySizeFinder[UUID](
      memoryStore = underlyingStore
    )

    testWith(sizeFinder)
  }

  override def createIdent(implicit underlyingStore: StoreImpl): UUID =
    UUID.randomUUID()

  override def createObject(ident: UUID, contents: String)(implicit underlyingStore: StoreImpl): Unit = {
    underlyingStore.put(ident)(contents.getBytes())
  }
}
