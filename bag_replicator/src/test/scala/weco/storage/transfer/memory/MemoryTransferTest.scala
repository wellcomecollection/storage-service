package weco.storage.transfer.memory

import weco.fixtures.TestWith
import weco.storage.store.fixtures.StringNamespaceFixtures
import weco.storage.store.memory.MemoryStore
import weco.storage.transfer.{Transfer, TransferTestCases}

class MemoryTransferTest
    extends TransferTestCases[
      String,
      String,
      Array[Byte],
      String,
      String,
      MemoryStore[String, Array[Byte]] with MemoryTransfer[String, Array[Byte]],
      MemoryStore[String, Array[Byte]] with MemoryTransfer[String, Array[Byte]],
      MemoryStore[String, Array[Byte]] with MemoryTransfer[String, Array[Byte]]]
    with MemoryTransferFixtures[String, Array[Byte]]
    with StringNamespaceFixtures {
  type MemoryStoreContext =
    MemoryStore[String, Array[Byte]] with MemoryTransfer[String, Array[Byte]]

  override def createSrcLocation(namespace: String): String =
    createId(namespace)

  override def createDstLocation(namespace: String): String =
    createId(namespace)

  override def createT: Array[Byte] = randomBytes()

  override def withSrcNamespace[R](testWith: TestWith[String, R]): R =
    testWith(randomAlphanumeric())

  override def withDstNamespace[R](testWith: TestWith[String, R]): R =
    testWith(randomAlphanumeric())

  override def withContext[R](testWith: TestWith[MemoryStoreContext, R]): R =
    withTransferStore(initialEntries = Map.empty) { store =>
      testWith(store)
    }

  override def withSrcStore[R](initialEntries: Map[String, Array[Byte]])(
    testWith: TestWith[MemoryStoreContext, R])(
    implicit context: MemoryStoreContext): R = {
    initialEntries.foreach {
      case (location, entry) =>
        context.put(location)(entry)
    }

    testWith(context)
  }

  override def withDstStore[R](initialEntries: Map[String, Array[Byte]])(
    testWith: TestWith[MemoryStoreContext, R])(
    implicit context: MemoryStoreContext): R = {
    initialEntries.foreach {
      case (location, value) =>
        context.put(location)(value)
    }

    testWith(context)
  }

  override def withTransfer[R](srcStore: MemoryStoreContext,
                               dstStore: MemoryStoreContext)(
    testWith: TestWith[Transfer[String, String], R]): R =
    testWith(
      srcStore
    )
}
