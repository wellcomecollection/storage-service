package weco.storage.transfer.fixtures

import weco.fixtures.TestWith
import weco.storage.store.Store
import weco.storage.transfer.Transfer

trait TransferFixtures[Ident, T, StoreImpl <: Store[Ident, T]] {
  def createT: T

  def withTransferStore[R](initialEntries: Map[Ident, T])(
    testWith: TestWith[StoreImpl, R]): R

  def withTransfer[R](testWith: TestWith[Transfer[Ident, Ident], R])(
    implicit store: StoreImpl): R
}
