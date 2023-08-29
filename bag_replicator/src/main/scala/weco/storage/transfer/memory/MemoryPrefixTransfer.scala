package weco.storage.transfer.memory

import weco.storage.listing.memory.MemoryListing
import weco.storage.transfer.PrefixTransfer

trait MemoryPrefixTransfer[Ident, Prefix, T]
    extends PrefixTransfer[Prefix, Ident, Prefix, Ident]
    with MemoryTransfer[Ident, T]
    with MemoryListing[Ident, Prefix, T] {
  implicit val transfer: MemoryTransfer[Ident, T] = this
  implicit val listing: MemoryListing[Ident, Prefix, T] = this
}
