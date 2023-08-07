package weco.storage.listing.memory

import weco.storage.listing.Listing
import weco.storage.store.memory.MemoryStoreBase
import weco.storage.listing.Listing
import weco.storage.store.memory.MemoryStoreBase

trait MemoryListing[Ident, Prefix, T]
    extends Listing[Prefix, Ident]
    with MemoryStoreBase[Ident, T] {

  override def list(prefix: Prefix): ListingResult = {
    val matchingIdentifiers = entries
      .filter { case (ident, _) => startsWith(ident, prefix) }
      .map { case (ident, _) => ident }

    Right(matchingIdentifiers)
  }

  protected def startsWith(id: Ident, prefix: Prefix): Boolean
}
