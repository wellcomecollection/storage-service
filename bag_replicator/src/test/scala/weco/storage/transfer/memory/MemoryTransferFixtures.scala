package weco.storage.transfer.memory

import weco.fixtures.TestWith
import weco.storage.store.memory.MemoryStore
import weco.storage.transfer.Transfer
import weco.storage.transfer.fixtures.TransferFixtures

trait MemoryTransferFixtures[Ident, T]
    extends TransferFixtures[
      Ident,
      T,
      MemoryStore[Ident, T] with MemoryTransfer[Ident, T]] {
  type MemoryStoreImpl = MemoryStore[Ident, T] with MemoryTransfer[Ident, T]

  override def withTransfer[R](testWith: TestWith[Transfer[Ident, Ident], R])(
    implicit store: MemoryStoreImpl): R =
    testWith(store)

  override def withTransferStore[R](initialEntries: Map[Ident, T])(
    testWith: TestWith[MemoryStoreImpl, R]): R = {
    val store = new MemoryStore[Ident, T](initialEntries)
    with MemoryTransfer[Ident, T]

    testWith(store)
  }
}
