package weco.storage.listing

import weco.storage.ListingFailure

trait Listing[Prefix, Result] {
  type ListingResult = Either[ListingFailure[Prefix], Iterable[Result]]

  def list(prefix: Prefix): ListingResult
}
